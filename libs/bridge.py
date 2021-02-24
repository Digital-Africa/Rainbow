# -*- coding: utf-8 -*-

import libs.DA_IO as DA_IO
import pandas
import re
import ast
import string
import datetime
import libs.GCP_management as gcp
from dateutil.relativedelta import relativedelta



class Bridge(object):
	"""docstring for Bridge"""
	def __init__(self, input_file):
		super(Bridge, self).__init__()
		self.local = "/Users/mohamedkabadiabakhate/Documents/Rainbow/Data/Raw"
		self.batch_0 = ["bridges-07-01-2021 15-30-30.xlsx","bridges-29-01-2021 10-50-31.xlsx","bridges-09-02-2021 08-18-47.xlsx"]
		self.input_file = input_file
		self.raw = pandas.read_excel('{}/{}'.format(self.local, self.input_file))
		self.export_folder = '/Users/mohamedkabadiabakhate/Documents/Rainbow/Data/DataStudio/Bridge/test'

	def norm_key(self, element):
		#lower()
		#Remove punct
		try:
			return element.lower().replace('www','').replace('https','').replace('http','').replace(' ','').translate(str.maketrans('', '', string.punctuation))
		except:
			return '__'

	def ancien(self, element):
		date_time_str = element
		date_time_obj = datetime.datetime.strptime(date_time_str, '%m/%Y')
		end_date, start_date = datetime.datetime.now(),date_time_obj
		difference_in_years = relativedelta(end_date, start_date).years
		return difference_in_years

	def process_raw(self):
		raw = self.raw
		raw['key'] = raw['Startup'].map(self.norm_key) + raw['Site web'].map(self.norm_key)
		raw = raw[raw['key'].isin(set(e for e in raw['key'] if 'test' not in e))].copy()
		raw['proportion CA Afr'] = raw["Chiffre d'affaire (Afrique)"].map(lambda x: float(x) if x!= ''  and x==x else 0)*100/raw["Chiffre d'affaire"].map(lambda x: float(x) if x!= '' and x== x else 0)
		raw['ancien'] = raw["Début de l'activité"].map(self.ancien)
		raw = raw.groupby('key').last().reset_index()
		raw = raw.copy()
		raw.columns = [e.lower().replace(' ', '_').replace('é','e'). replace("'","").replace('(','').replace(')','').replace('.','').replace('û','u').replace('è','e') for e in raw.keys()]
		for element in raw:
			raw[element] = raw[element].map(lambda x: "{}".format(x.replace('\r\n', '').replace('\r', '').replace('\n', '')) if type(x)==str else x)
		return raw

	def process_secteurs(self):
		raw = self.process_raw()
		secteurs = raw[['key','secteurs']]
		t= [[key, [element for element in secteur.split(', ') if element != '']] for key, secteur in secteurs.values.tolist()]
		Secteur_output = []
		for key, vals in t:
			for sect in vals:
				Secteur_output.append([key,sect])
		Secteur_output = pandas.DataFrame(Secteur_output, columns = ['key', 'Secteur']).dropna()
		return Secteur_output

	def process_competences(self):
		raw = self.process_raw()
		competences = raw[['key', 'competences','autre_competence']]
		t= [[key, [element for element in competence.split(', ')+[autre] if element != '']] for key, competence, autre in competences.values.tolist()]

		output = []

		for key, vals in t:
			for compt in vals:
				output.append([key,compt])
		Competence_output = pandas.DataFrame(output, columns = ['key', 'Competence']).dropna()		
		return Competence_output

	def process_typefinancements(self):
		raw = self.process_raw()		
		financement = raw[['key', 'types_de_financement','autre_type_de_financement']]
		t= [[key, [element for element in fi.split(',')+[autre] if element != '']] for key, fi, autre in financement.values.tolist() if fi == fi]
		output = []
		for key, vals in t:
			for compt in vals:
				output.append([key,compt])
		type_financement_output = pandas.DataFrame(output, columns = ['key', "type_financement"]).dropna()
		return type_financement_output

	def process_coutsfixes(self):
		raw = self.process_raw()
		cout = raw[['key', 'cout_fixe']]
		t= [[key, ast.literal_eval(secteur)] for key, secteur in cout.values.tolist() if secteur == secteur]
		output = []
		for key, vals in t:
			for sect in vals:
				output.append([key,sect])
		Couts_fixes_output = pandas.DataFrame(output, columns = ['key', 'Couts_fixes']).dropna()				
		return Couts_fixes_output 


	def process_repartitionlinear_secteurs(self):
		raw = self.process_raw()
		left = raw[['key','startup', 'ancien','siege','chiffre_daffaire_recurent','chiffre_daffaire_afrique','chiffre_daffaire','proportion_ca_afr','eligible','poster_le']].copy()
		left = left.set_index('key')
		secteur = self.process_secteurs().set_index('key')
		master = left.join(secteur)
		master = master.join(master.groupby('key').count()['Secteur'],rsuffix = '_unit')
		master['linear_chiffre_daffaire_recurent'] = master["chiffre_daffaire_recurent"] / master["Secteur_unit"]
		master['linear_chiffre_daffaire'] = master["chiffre_daffaire"] / master["Secteur_unit"]
		master['linear_chiffre_dafaire_afrique'] = master["chiffre_daffaire_afrique"] / master["Secteur_unit"]
		master['eligible'] = master['eligible'].map(lambda x: x == True if x > 0 else False)
		return master

	def process_repartitionlinear_competences(self):
		raw = self.process_raw()
		left = raw[['key','startup', 'ancien','siege','chiffre_daffaire_recurent','chiffre_daffaire_afrique','chiffre_daffaire','proportion_ca_afr','eligible','poster_le']].copy()
		left = left.set_index('key')
		skills = self.process_competences().set_index('key')
		master = left.join(skills)
		master = master.join(master.groupby('key').count()['Competence'],rsuffix = '_unit')
		master['linear_chiffre_daffaire_recurent'] = master["chiffre_daffaire_recurent"] / master["Competence_unit"]
		master['linear_chiffre_daffaire'] = master["chiffre_daffaire"] / master["Competence_unit"]
		master['linear_chiffre_dafaire_afrique'] = master["chiffre_daffaire_afrique"] / master["Competence_unit"]
		master['eligible'] = master['eligible'].map(lambda x: x == True if x > 0 else False)
		return master

	def process_repartitionlinear_couts_fixes(self):
		raw = self.process_raw()
		left = raw[['key','startup', 'ancien','siege','chiffre_daffaire_recurent','chiffre_daffaire_afrique','chiffre_daffaire','proportion_ca_afr','eligible','poster_le']].copy()
		left = left.set_index('key')
		coutsfixes = self.process_coutsfixes().set_index('key')
		master = left.join(coutsfixes)
		master = master.join(master.groupby('key').count()['Couts_fixes'],rsuffix = '_unit')
		master['linear_chiffre_daffaire_recurent'] = master["chiffre_daffaire_recurent"] / master["Couts_fixes_unit"]
		master['linear_chiffre_daffaire'] = master["chiffre_daffaire"] / master["Couts_fixes_unit"]
		master['linear_chiffre_dafaire_afrique'] = master["chiffre_daffaire_afrique"] / master["Couts_fixes_unit"]
		master['eligible'] = master['eligible'].map(lambda x: x == True if x > 0 else False)
		return master


	def export_csv(self):
		raw = self.process_raw()
		raw.to_csv('{}/{}.csv'.format(self.export_folder,'raw'))
		self.process_secteurs().to_csv('{}/{}.csv'.format(self.export_folder,'secteurs'))
		self.process_competences().to_csv('{}/{}.csv'.format(self.export_folder,'competences'))
		self.process_typefinancements().to_csv('{}/{}.csv'.format(self.export_folder,'typefinancements'))
		self.process_coutsfixes().to_csv('{}/{}.csv'.format(self.export_folder,'coutsfixes'))
		self.process_repartitionlinear_secteurs().to_csv('{}/{}.csv'.format(self.export_folder,'repartionlinearsecteur'))
		self.process_repartitionlinear_competences().to_csv('{}/{}.csv'.format(self.export_folder,'repartionlinearcompetences'))
		self.process_repartitionlinear_couts_fixes().to_csv('{}/{}.csv'.format(self.export_folder,'repartionlinearcoutsfixes'))

	def export_Bigquery(self, project_id = 'digital-africa-rainbow', dataset_id = 'DataStudio', if_exists = 'replace'):
		bq = gcp.Bigquery_management()
		raw = self.process_raw()
		secteurs = self.process_secteurs()
		competences = self.process_competences()
		typefinancements = self.process_typefinancements()
		coutsfixes = self.process_coutsfixes()
		repartitionlinear_secteurs = self.process_repartitionlinear_secteurs()
		repartitionlinear_competences = self.process_repartitionlinear_competences()
		repartitionlinear_couts_fixes = self.process_repartitionlinear_couts_fixes()
		bq.upload_dataframe(df = raw.reset_idex(), project_id = project_id, dataset_id = dataset_id, table_id ='Bridge_Raw', if_exists = if_exists)		
		bq.upload_dataframe(df = secteur.reset_idex(), project_id = project_id, dataset_id = dataset_id, table_id ='Bridge_secteurs', if_exists = if_exists)
		bq.upload_dataframe(df = competences.reset_idex(), project_id = project_id, dataset_id = dataset_id, table_id ='Bridge_competences', if_exists = if_exists)
		bq.upload_dataframe(df = typefinancements.reset_idex(), project_id = project_id, dataset_id = dataset_id, table_id ='Bridge_typefinancements', if_exists = if_exists)
		bq.upload_dataframe(df = coutsfixes.reset_idex(), project_id = project_id, dataset_id = dataset_id, table_id ='Bridge_coutsfixes', if_exists = if_exists)
		bq.upload_dataframe(df = repartitionlinear_secteurs.reset_idex(), project_id = project_id, dataset_id = dataset_id, table_id ='Bridge_repartitionlinear_secteurs', if_exists = if_exists)
		bq.upload_dataframe(df = repartitionlinear_competences.reset_idex(), project_id = project_id, dataset_id = dataset_id, table_id ='Bridge_repartitionlinear_competences', if_exists = if_exists)
		bq.upload_dataframe(df = repartitionlinear_couts_fixes.reset_idex(), project_id = project_id, dataset_id = dataset_id, table_id ='Bridge_repartitionlinear_couts_fixes', if_exists = if_exists)


	def export_Gsheet():
		export_folder = '/Users/mohamedkabadiabakhate/Documents/Rainbow/Data/DataStudio/Bridge'
		raw = process_raw(raw_data)
		raw.to_csv('{}/{}.csv'.format(export_folder,'raw'))
		process_secteurs(raw).to_csv('{}/{}.csv'.format(export_folder,'secteurs'))
		process_competences(raw).to_csv('{}/{}.csv'.format(export_folder,'competences'))
		process_typefinancements(raw).to_csv('{}/{}.csv'.format(export_folder,'typefinancements'))
		process_coutsfixes(raw).to_csv('{}/{}.csv'.format(export_folder,'coutsfixes'))
		process_repartitionlinear_secteurs(raw).to_csv('{}/{}.csv'.format(export_folder,'repartionlinearsecteur'))
		process_repartitionlinear_competences(raw).to_csv('{}/{}.csv'.format(export_folder,'repartionlinearcompetences'))
		process_repartitionlinear_couts_fixes(raw).to_csv('{}/{}.csv'.format(export_folder,'repartionlinearcoutsfixes'))


	def export_Gstorage():
		export_folder = '/Users/mohamedkabadiabakhate/Documents/Rainbow/Data/DataStudio/Bridge'
		raw = process_raw(raw_data)
		raw.to_csv('{}/{}.csv'.format(export_folder,'raw'))
		process_secteurs(raw).to_csv('{}/{}.csv'.format(export_folder,'secteurs'))
		process_competences(raw).to_csv('{}/{}.csv'.format(export_folder,'competences'))
		process_typefinancements(raw).to_csv('{}/{}.csv'.format(export_folder,'typefinancements'))
		process_coutsfixes(raw).to_csv('{}/{}.csv'.format(export_folder,'coutsfixes'))
		process_repartitionlinear_secteurs(raw).to_csv('{}/{}.csv'.format(export_folder,'repartionlinearsecteur'))
		process_repartitionlinear_competences(raw).to_csv('{}/{}.csv'.format(export_folder,'repartionlinearcompetences'))
		process_repartitionlinear_couts_fixes(raw).to_csv('{}/{}.csv'.format(export_folder,'repartionlinearcoutsfixes'))
