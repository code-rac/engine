# -*- coding: UTF-8 -*-
import re, json
from elasticsearch import helpers, Elasticsearch
from pprint import pprint

class Wasl: # WASL: WebAssistant Searching language
	def __init__(self, es):
		self.es = es

	def scroll(self, waslquery, index='*', start_at=None, end_at=None):
		# @return: yield a generator to scroll all docs
		query = self.wasl2elasticsearch(waslquery, start_at=start_at, end_at=end_at)
		if query is not None:
			if 'aggs' in query:
				del query['aggs']
			generator = helpers.scan(self.es, index=index, scroll='1m', query=query, request_timeout=120, preserve_order=True, raise_on_error=False)
			return generator
		return None

	def wasl2elasticsearch(self, command = "", start_at=None, end_at=None):
		count = 1
		aggs  = None
		aggs_level = 0
		# Ham nay dung de chuyen cau truy van dang string (duoc nhap tu nguoi dung) ve dang cau truy van trong Elasticsearch
		# @command : Cau lenh tim kiem
		# @return: Return a json query for elasticsearch, return None when the command is invalid.

		commandQuery = {
			"query" : {
				"bool" :{
					"must" : [],
					"must_not" : [],
					"should" : [],
				}
			},
			"aggs" : {}
		};

		# khai bao cac truong du lieu duoc accepted in the searching query
		fields = ["count", "useragent", "os", "method","url", "browser", "path", "query", "byte", "status", "client_ip", "http_version", "country", "referer", "country_code", "user", "time", "hour", "aggs"]

		# this mapping to configuration mapping type of fields
		mapping_fields = {
			"method":{"field":"method.raw","type":"match","type_data":"string"},
			"useragent":{"field":"useragent.raw","type":"fuzzy","type_data":"string"},
			"browser":{"field":"browser.raw","type":"fuzzy","type_data":"string"},
			"os":{"field":"os.raw","type":"fuzzy","type_data":"string"},
			"path":{"field":"target.raw","type":"regexp","type_data":"string"},
			"url":{"field":"url.raw","type":"regexp","type_data":"string"},
			"query":{"field":"query.raw","type":"regexp","type_data":"string"},
			"http_version":{"field":"http_version.raw","type":"regexp","type_data":"string"},
			"byte":{"field":"byte_tx","type":"match","type_data":"number"},
			"referer":{"field":"referer.raw","type":"regexp","type_data":"string"},
			"status":{"field":"status_code","type":"match","type_data":"number"},
			"country":{"field":"country.raw","type":"regexp","type_data":"string"},
			"country_code":{"field":"country_code","type":"match","type_data":"string"},
			"user":{"field":"remote_user.raw","type":"match","type_data":"string"},
			"client_ip":{"field":"remote_host.raw","type":"regexp","type_data":"string"},
			"hour":{"field":"hour","type":"regexp","type_data":"number"},
			"time":{"field":"time","type":"regexp","type_data":"date"},
			"raw":{"field":"raw","type":"regexp","type_data":"string"}
		}
		
		# command = strtolower(command);
		if command != "":

			# tach cau truy van thanh tung pharse
			elements = command.split(" | ")
			# Chi su dung mot cau simple query string
			simple_query_string = "";

			for  item in elements:
				item_str = item;
				# token = ['=', '!=', '>', '<'] # cac token duoc chap nhan (Luu y xem theo thu tu)
				key_values = re.split(r">=|<=|!=|=|>|<", item); # Tac tiep tung item.
				if len(key_values) == 1:
					matchObj = re.match("^aggs\((?P<key>.+)\)", item, re.M)
					if matchObj:
						key = matchObj.group('key').split()[0]
						if key in fields:
							commandQuery["aggs"] = self.aggs(commandQuery["aggs"], {
								"was-aggs" : {
									'terms' : {
										'field' :  mapping_fields[key]["field"],
										# text_token : int(value)
									}
								}
							})
					else:
						if simple_query_string == "":
							simple_query_string = item_str
						else:
							simple_query_string += " AND " + item_str;
				else:
					matchObj = re.match("^(?P<function>aggs|count)\((?P<key>.+)\)\s*(?P<token>>=|<=|!=|=|>|<)\s*(?P<value>.+)\s*", item, re.M)
					if matchObj:
						key = matchObj.group('key').split()[0]
						token = matchObj.group('token').split()[0]
						value = matchObj.group('value').split()[0]
						function = matchObj.group('function').split()[0]
						text_token = "min_doc_count"
						if token == ">":
							text_token = "min_doc_count"
						elif token == "<":
							text_token = "max_doc_count"
						elif token == "=":
							text_token = "size"
						else:
							return None

						if function == "aggs":
							commandQuery["aggs"] = self.aggs(commandQuery["aggs"], {
									"was-aggs" : {
										'terms' : {
											'field' :  mapping_fields[key]["field"],
											text_token : int(value)
										}
									}
								})
						elif function == "count":
							commandQuery["aggs"] = self.aggs(commandQuery["aggs"], {
									"was-count" : {
										'date_histogram' : {
											"field" :  "arrived_time",
											"interval" : key,
											text_token : int(value)
										}
									}
								})
						else:
							return None

					else:
						# Xu ly cac pattern dang: status>300 | url=regex(.*login.*)
						matchObj = re.match("^(?P<key>[^>=<!]+)(?P<token>>=|<=|!=|=|>|<)(?P<value>.+)\s*", item)
						if matchObj:
							key = matchObj.group('key').split()[0]
							token = matchObj.group('token').split()[0]
							value = matchObj.group('value').split()[0]
							text_token = "lte"
							if token == "=":
								text_token = "equal"
							elif token == ">":
								text_token = "gt"
							elif token == ">=":
								text_token = "gte"
							elif token == "<":
								text_token = "lt"
							elif token == "<=":
								text_token = "lte"
							elif token == "!=":
								text_token = "not";

							if key in fields:
								# Xu ly cac dieu kien so sanh ve thoi gian, kieu numberic
								if mapping_fields[key]["type_data"] == "number" or mapping_fields[key]["type_data"] == "date":
										if text_token  in ["gt", "lt", "lte", "gte"]:
											if (mapping_fields[key]["type_data"] == "number"):
												term = {
													"range" : {
														mapping_fields[key]["field"] : {
															text_token :  int(value)
															}
														}
													}
											else:
												term = {"range" : {mapping_fields[key]["field"] : {text_token : value}}}
			
											commandQuery["query"]["bool"]["must"].append(term)
										elif text_token in ["equal", "not"]:
											if mapping_fields[key]["type_data"] == "number":
												term = {
													"term" : {	
														mapping_fields[key]["field"] : int(value)
													}
												}
											else:
												term = {"term" : {mapping_fields[key]["field"] : value}};

											if term:
												if (text_token == "equal"):
													commandQuery["query"]["bool"]["must"].append(term)
												else:
													commandQuery["query"]["bool"]["must_not"].append(term)

								if mapping_fields[key]["type_data"] == "string":
									if text_token in ["equal", "not"]:
										type_match = False
										functions = ["regex", "wildcard", "fuzzy"]
										for f in functions:
											if value[0 : len(f)] == f:
												type_match = True
												newvalue = value[len(f) + 1 : -1]
												if f == "regex":
													f = "regexp"
												
												term = {f : {mapping_fields[key]["field"] : newvalue}};
										if not type_match:
											regex = "^\"(?P<param>.+)\"\s*"
											matchObj = re.match(regex, value, re.M)
											if matchObj:
												term = {"match" : {mapping_fields[key]["field"] : matchObj.group(["param"])}}
											else:
												term = {"prefix" : {mapping_fields[key]["field"] : value}};

										if term:
											if text_token == "equal":
												commandQuery["query"]["bool"]["must"].append(term)
											else:
												commandQuery["query"]["bool"]["must_not"].append(term)
											

							else:
								if (simple_query_string == ""):
									simple_query_string = item_str
								else:
									simple_query_string+= " AND "+item_str;
								
							
						else:
							if (simple_query_string == ""):
								simple_query_string = item_str
							else:
								simple_query_string+= " AND "+item_str

			if len(simple_query_string) > 0:
				commandQuery["query"]["bool"]["must"].append({"simple_query_string" : {"query" : simple_query_string}})

		if start_at:
			commandQuery["query"]["bool"]["must"].append({"range" : {"arrived_time" : {"gte":start_at}}})
		if end_at:
			commandQuery["query"]["bool"]["must"].append({"range" : {"arrived_time" : {"lt":end_at}}})
		return commandQuery

