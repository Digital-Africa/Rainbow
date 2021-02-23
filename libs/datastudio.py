# -*- coding: utf-8 -*-

import libs.DA_IO as DA_IO
import pandas
import re
import ast
import string
import datetime
import libs.GCP_management as gcp
from dateutil.relativedelta import relativedelta



local = "/Users/mohamedkabadiabakhate/Documents/Rainbow/Data/Raw"
batch_0 = ["bridges-07-01-2021 15-30-30.xlsx","bridges-29-01-2021 10-50-31.xlsx","bridges-09-02-2021 08-18-47.xlsx"]
input_file = "bridges-09-02-2021 08-18-47.xlsx"
raw = pandas.read_excel('{}/{}'.format(local, input_file))




def norm_key(element):
    #lower()
    #Remove punct
    try:
        return element.lower().replace('www','').replace('https','').replace('http','').replace(' ','').translate(str.maketrans('', '', string.punctuation))
    except:
        return '__'


def ancien(element):
    date_time_str = element
    date_time_obj = datetime.datetime.strptime(date_time_str, '%m/%Y')
    end_date, start_date = datetime.datetime.now(),date_time_obj
    difference_in_years = relativedelta(end_date, start_date).years
    return difference_in_years




def process_raw(raw):
	raw['key'] = raw['Startup'].map(norm_key) + raw['Site web'].map(norm_key)
	raw = raw[raw['key'].isin(set(e for e in raw['key'] if 'test' not in e))].copy()
	raw['proportion CA Afr'] = raw["Chiffre d'affaire (Afrique)"].map(lambda x: float(x) if x!= ''  and x==x else 0)*100/raw["Chiffre d'affaire"].map(lambda x: float(x) if x!= '' and x== x else 0)
	raw['ancien'] = raw["Début de l'activité"].map(ancien)
	raw = raw.groupby('key').last().reset_index()
	raw = raw.copy()
	raw.columns = [e.lower().replace(' ', '_').replace('é','e'). replace("'","").replace('(','').replace(')','').replace('.','').replace('û','u').replace('è','e') for e in raw.keys()]
	for element in raw:
	    raw[element] = raw[element].map(lambda x: "{}".format(x.replace('\r\n', '').replace('\r', '').replace('\n', '')) if type(x)==str else x)
	return raw


def process_secteurs(raw):
	secteurs = raw[['key','secteurs']]

	t= [[key, [element for element in secteur.split(', ') if element != '']] for key, secteur in secteurs.values.tolist()]

	Secteur_output = []

	for key, vals in t:
	    for sect in vals:
	        Secteur_output.append([key,sect])

	Secteur_output = pandas.DataFrame(Secteur_output, columns = ['key', 'Secteur']).dropna()

	return Secteur_output

def process_competences(raw):
	competences = raw[['key', 'competences','autre_competence']]
	t= [[key, [element for element in competence.split(', ')+[autre] if element != '']] for key, competence, autre in competences.values.tolist()]

	output = []

	for key, vals in t:
	    for compt in vals:
	        output.append([key,compt])
	Competence_output = pandas.DataFrame(output, columns = ['key', 'Competence']).dropna()
	
	return Competence_output

def process_typefinancements(raw):
	financement = raw[['key', 'types_de_financement','autre_type_de_financement']]
	t= [[key, [element for element in fi.split(',')+[autre] if element != '']] for key, fi, autre in financement.values.tolist() if fi == fi]

	output = []

	for key, vals in t:
	    for compt in vals:
	        output.append([key,compt])
	type_financement_output = pandas.DataFrame(output, columns = ['key', "type_financement"]).dropna()

	return type_financement_output

def process_coutsfixes(raw):
	cout = raw[['key', 'cout_fixe']]

	t= [[key, ast.literal_eval(secteur)] for key, secteur in cout.values.tolist() if secteur == secteur]

	output = []

	for key, vals in t:
	    for sect in vals:
	        output.append([key,sect])
	Couts_fixes_output = pandas.DataFrame(output, columns = ['key', 'Couts_fixes']).dropna()				

	return Couts_fixes_output 


def process_repartitionlinear_secteurs(raw):
	left = raw[['key','ancien','siege','chiffre_daffaire_recurent','chiffre_daffaire_afrique','chiffre_daffaire','proportion_ca_afr','eligible','poster_le']].copy()
	left = left.set_index('key')
	secteur = process_secteurs(raw).set_index('key')

	master = left.join(secteur)
	master = master.join(master.groupby('key').count()['Secteur'],rsuffix = '_unit')
	master['linear_chiffre_daffaire_recurent'] = master["chiffre_daffaire_recurent"] / master["Secteur_unit"]
	master['linear_chiffre_daffaire'] = master["chiffre_daffaire"] / master["Secteur_unit"]
	master['linear_chiffre_dafaire_afrique'] = master["chiffre_daffaire_afrique"] / master["Secteur_unit"]
	master['eligible'] = master['eligible'].map(lambda x: x == True if x > 0 else False)
	return master

def process_repartitionlinear_competences(raw):
	left = raw[['key','ancien','siege','chiffre_daffaire_recurent','chiffre_daffaire_afrique','chiffre_daffaire','proportion_ca_afr','eligible','poster_le']].copy()
	left = left.set_index('key')
	skills = process_competences(raw).set_index('key')

	master = left.join(skills)
	master = master.join(master.groupby('key').count()['Competence'],rsuffix = '_unit')
	master['linear_chiffre_daffaire_recurent'] = master["chiffre_daffaire_recurent"] / master["Competence_unit"]
	master['linear_chiffre_daffaire'] = master["chiffre_daffaire"] / master["Competence_unit"]
	master['linear_chiffre_dafaire_afrique'] = master["chiffre_daffaire_afrique"] / master["Competence_unit"]
	master['eligible'] = master['eligible'].map(lambda x: x == True if x > 0 else False)
	return master

