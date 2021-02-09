import challenge1000 as ch
import uuid

ch1000 = ch.Native()
output = '/Volumes/GoogleDrive/.shortcut-targets-by-id/18Mfm8xy_KmlL-IR-EYmWCKzANHjmoCPu/2020/Rainbow/Data/{}/{}'
output_header = ['categorie', 
'nom_struc', 
'prez_duplicable_struc', 
'prez_duplicable_struc_trad', 
'prez_durable_struc', 
'prez_durable_struc_trad',
'prez_innovante_struc',
'prez_innovante_struc_trad',
'prez_marche_struc',
'prez_marche_struc_trad',
'prez_objectif_struc',
'prez_objectif_struc_trad',
'prez_produit_struc',
'prez_produit_struc_trad',
'prez_struc',
'prez_struc_trad',
'prez_zone_struc',
'prez_zone_struc_trad','uuid']

X = ch1000.X
Y = ch1000.descriptions_trad
Z = ch1000.descriptions
Z = Z.join(Y, rsuffix = '_trad').dropna().reset_index()
Z['uuid'] = Z['key_main'].map(lambda x: uuid.uuid1())

def save(folder, filename):
  Z[['uuid', 'key_main']].to_csv(output.format('UUID','uuid_keymain_challenge1000_old.csv'))
  Z[output_header].to_csv(output.format('TrainingSet', 'training_set_l1.csv'))

