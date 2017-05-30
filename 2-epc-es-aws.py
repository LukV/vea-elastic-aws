#!/usr/bin/env python
import argparse
import sys
import pprint
import csv
import json
import requests
import datetime
import time
from collections import OrderedDict
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth

def main(args):
	global es

	if args.host == 'local':
		es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
	elif args.host == 'aws':
		awsauth = AWS4Auth('', '', 'eu-west-1', 'es')
		es = Elasticsearch(
			hosts=[{'host':'', 'port': 443}],
			http_auth=awsauth,
			use_ssl=True,
			verify_certs=True,
			connection_class=RequestsHttpConnection
		)

	if args.input:
		doimport(args)
	elif args.printdoc:
		printdoc(args)
	elif args.createindex:
		createindex(args)

def createindex(args):
	es.indices.delete(index="epc-pub", ignore=[400, 404])
	es.indices.create(index="epc-pub", ignore=400)
	print('epc-pub index created')

def printdoc(args):
	res = es.get(index="epc-pub", doc_type='attest', id=args.printdoc)
	print(res['_source'])

def doimport(args):
	oldKey = None
	metingen = []
	
	with open(args.input, 'r') as f:
		reader = csv.DictReader(f, delimiter='|')
		for idx, line in enumerate(reader):
			thisKey = line['EPC_ID']

			if oldKey and oldKey != thisKey:
				doc = {}
				doc['id'] = oldKey
				doc['metingen']  = metingen
				doc['status'] = status
				doc['label'] = naam_organisatie
				doc['type_publiek_gebouw'] = type_publiek_gebouw
				doc['datum_ingediend'] = datum_ingediend
				doc['start_meting'] = start_meting
				doc['einde_meting'] = einde_meting
				doc['bouwjaar'] = bouwjaar
				if len(verbouwjaar) > 0: doc['verbouwjaar'] = verbouwjaar
				if len(bruikbare_vloeroppervlakte) > 0: doc['bruikbare_vloeroppervlakte'] = bruikbare_vloeroppervlakte
				if len(epc_kengetal) > 0: doc['epc_kengetal'] = epc_kengetal
				if len(gemeente) > 0: doc['gemeente'] = gemeente
				if len(postcode) > 0: doc['postcode'] = postcode
				if len(straat) > 0: doc['straat'] = straat
				if len(nummer) > 0: doc['nummer'] = nummer
				if len(bus) > 0: doc['bus'] = bus
				if len(crab_gemeente_id) > 0: doc['crab_gemeente_id'] = crab_gemeente_id 
				if len(crab_straat_id) > 0: doc['crab_straat_id'] = crab_straat_id 
				if len(crab_huisnummer_id) > 0: doc['crab_huisnummer_id'] = crab_straat_id  
				if len(crab_subadres_id) > 0: doc['crab_subadres_id'] = crab_subadres_id 
				if len(administr_koelinstallatie) > 0: doc['administr_koelinstallatie'] = administr_koelinstallatie 
				if len(administr_serverlokaal) > 0: doc['administr_serverlokaal'] = administr_serverlokaal 
				if len(administr_aantal_personen) > 0: doc['administr_aantal_personen'] = administr_aantal_personen 
				if len(administr_aantal_maaltijden) > 0: doc['administr_aantal_maaltijden'] = administr_aantal_maaltijden 
				if len(onderwijs_aantal_lln) > 0: doc['onderwijs_aantal_lln'] = onderwijs_aantal_lln
				if len(onderwijs_aantal_m2_sporthal) > 0: doc['onderwijs_aantal_m2_sporthal'] = onderwijs_aantal_m2_sporthal 
				if len(onderwijs_aantal_m2_zwembad) > 0: doc['onderwijs_aantal_m2_zwembad'] = onderwijs_aantal_m2_zwembad 
				if len(onderwijs_aantal_maaltijden) > 0: doc['onderwijs_aantal_maaltijden'] = onderwijs_aantal_maaltijden
				if len(opvang_aantal_kinderen) > 0: doc['opvang_aantal_kinderen'] = opvang_aantal_kinderen
				if len(rusthuis_aantal_bedden) > 0: doc['rusthuis_aantal_bedden'] = rusthuis_aantal_bedden
				if len(rusthuis_serviceflats) > 0: doc['rusthuis_serviceflats'] = rusthuis_serviceflats
				if len(ziekehuis_aantal_bedden) > 0: doc['ziekehuis_aantal_bedden'] = ziekehuis_aantal_bedden
				if len(zwembad_wateroppervlakte) > 0: doc['zwembad_wateroppervlakte'] = zwembad_wateroppervlakte
				if len(zwembad_subtropisch) > 0: doc['zwembad_subtropisch'] = zwembad_subtropisch
			
				if len(status):
					jsonified = json.dumps(doc, indent=4, sort_keys=True)
					es.index(index='epc-pub', doc_type='attest', id=doc['id'], body=jsonified)
					print 'indexed item %s with id %s' % (idx, doc['id'])
					
				metingen = []

			oldKey = thisKey
		
			status = line['STATUS']
			naam_organisatie = line['NAAM_ORGANISATIE']
			type_publiek_gebouw = line['TYPE_PUBLIEK_GEBOUW']
			datum_ingediend = line['DATUM_INGEDIEND']
			start_meting = line['START_METING']
			einde_meting = line['EINDE_METING']
			bouwjaar = line['BOUWJAAR']
			verbouwjaar = line['VERBOUWJAAR']
			bruikbare_vloeroppervlakte = line['BRUIKBARE_VLOEROPPERVLAKTE']
			epc_kengetal = line['EPC_KENGETAL']
			gemeente = line['GEMEENTE']
			postcode = line['POSTCODE']
			straat = line['STRAAT']
			nummer = line['NUMMER']
			bus = line['BUS']
			crab_gemeente_id = line['CRAB_GEMEENTE_ID']
			crab_straat_id = line['CRAB_STRAAT_ID']
			crab_huisnummer_id = line['CRAB_HUISNUMMER_ID']
			crab_subadres_id = line['CRAB_SUBADRES_ID']
			administr_koelinstallatie = line['ADMINISTR_KOELINSTALLATIE']
			administr_serverlokaal = line['ADMINISTR_SERVERLOKAAL']
			administr_aantal_personen = line['ADMINISTR_AANTAL_PERSONEN']
			administr_aantal_maaltijden = line['ADMINISTR_AANTAL_MAALTIJDEN']
			onderwijs_aantal_lln = line['ONDERWIJS_AANTAL_LLN']
			onderwijs_aantal_m2_sporthal = line['ONDERWIJS_AANTAL_M2_SPORTHAL']
			onderwijs_aantal_m2_zwembad = line['ONDERWIJS_AANTAL_M2_ZWEMBAD']
			onderwijs_aantal_maaltijden = line['ONDERWIJS_AANTAL_MAALTIJDEN']
			opvang_aantal_kinderen = line['OPVANG_AANTAL_KINDEREN']
			rusthuis_aantal_bedden = line['RUSTHUIS_AANTAL_BEDDEN']
			rusthuis_serviceflats = line['RUSTHUIS_SERVICEFLATS']
			ziekehuis_aantal_bedden = line['ZIEKENHUIS_AANTAL_BEDDEN']
			zwembad_wateroppervlakte = line['ZWEMBAD_WATEROPPERVLAKTE']
			zwembad_subtropisch = line['ZWEMBAD_SUBTROPISCH']

			meting = {}
			meting['energiedrager'] = line['ENERGIEDRAGER']
			meting['waarde'] = line['WAARDE']
			meting['eenheid'] = line['EENHEID']
			metingen.append(meting)
		
		if oldKey != None:
			doc = {}
			doc['id'] = oldKey
			doc['metingen']  = metingen
			doc['status'] = status
			doc['label'] = naam_organisatie
			doc['type_publiek_gebouw'] = type_publiek_gebouw
			doc['datum_ingediend'] = datum_ingediend
			doc['start_meting'] = start_meting
			doc['einde_meting'] = einde_meting
			doc['bouwjaar'] = bouwjaar
			if len(verbouwjaar) > 0: doc['verbouwjaar'] = verbouwjaar
			if len(bruikbare_vloeroppervlakte) > 0: doc['bruikbare_vloeroppervlakte'] = bruikbare_vloeroppervlakte
			if len(epc_kengetal) > 0: doc['epc_kengetal'] = epc_kengetal
			if len(gemeente) > 0: doc['gemeente'] = gemeente
			if len(postcode) > 0: doc['postcode'] = postcode
			if len(straat) > 0: doc['straat'] = straat
			if len(nummer) > 0: doc['nummer'] = nummer
			if len(bus) > 0: doc['bus'] = bus
			if len(crab_gemeente_id) > 0: doc['crab_gemeente_id'] = crab_gemeente_id 
			if len(crab_straat_id) > 0: doc['crab_straat_id'] = crab_straat_id 
			if len(crab_huisnummer_id) > 0: doc['crab_huisnummer_id'] = crab_straat_id  
			if len(crab_subadres_id) > 0: doc['crab_subadres_id'] = crab_subadres_id 
			if len(administr_koelinstallatie) > 0: doc['administr_koelinstallatie'] = administr_koelinstallatie 
			if len(administr_serverlokaal) > 0: doc['administr_serverlokaal'] = administr_serverlokaal 
			if len(administr_aantal_personen) > 0: doc['administr_aantal_personen'] = administr_aantal_personen 
			if len(administr_aantal_maaltijden) > 0: doc['administr_aantal_maaltijden'] = administr_aantal_maaltijden 
			if len(onderwijs_aantal_lln) > 0: doc['onderwijs_aantal_lln'] = onderwijs_aantal_lln
			if len(onderwijs_aantal_m2_sporthal) > 0: doc['onderwijs_aantal_m2_sporthal'] = onderwijs_aantal_m2_sporthal 
			if len(onderwijs_aantal_m2_zwembad) > 0: doc['onderwijs_aantal_m2_zwembad'] = onderwijs_aantal_m2_zwembad 
			if len(onderwijs_aantal_maaltijden) > 0: doc['onderwijs_aantal_maaltijden'] = onderwijs_aantal_maaltijden
			if len(opvang_aantal_kinderen) > 0: doc['opvang_aantal_kinderen'] = opvang_aantal_kinderen
			if len(rusthuis_aantal_bedden) > 0: doc['rusthuis_aantal_bedden'] = rusthuis_aantal_bedden
			if len(rusthuis_serviceflats) > 0: doc['rusthuis_serviceflats'] = rusthuis_serviceflats
			if len(ziekehuis_aantal_bedden) > 0: doc['ziekehuis_aantal_bedden'] = ziekehuis_aantal_bedden
			if len(zwembad_wateroppervlakte) > 0: doc['zwembad_wateroppervlakte'] = zwembad_wateroppervlakte
			if len(zwembad_subtropisch) > 0: doc['zwembad_subtropisch'] = zwembad_subtropisch
		
			if len(status):
				jsonified = json.dumps(doc, indent=4, sort_keys=True)
				es.index(index='epc-pub', doc_type='attest', id=doc['id'], body=jsonified)
				print 'indexed item %s with id %s' % (success, doc['id'])
				success = success + 1

if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	parser.add_argument('-i','--input', help='Input CSV file')
	parser.add_argument('-p','--printdoc', help='Output JSON doc from ES')
	parser.add_argument('-c','--createindex', help='Creates index. Drops if exists.', action='store_true')
	parser.add_argument('-host','--host', help='local OR aws', required=True)
	main(parser.parse_args())
