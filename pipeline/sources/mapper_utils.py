
import re
from dateutil import parser
from dateparser.date import DateDataParser
from edtf import parse_edtf, text_to_edtf, struct_time_to_datetime
from datetime import datetime, timedelta
from edtf.parser.parser_classes import UncertainOrApproximate as UOA, \
	PartialUncertainOrApproximate as PUOA, MaskedPrecision as MP

default_dt = datetime.strptime("0001-01-01T00:00:00", "%Y-%m-%dT%H:%M:%S")
dp_settings = {'PREFER_DAY_OF_MONTH': 'first', 'PREFER_DATES_FROM': 'past'}
dp_parser = DateDataParser(settings=dp_settings)
x_re = re.compile('([?xXu0-9-])([?X])')

def make_datetime(value):
	# given a date / datetime string
	# return (begin, end) range

	if not value or value == '9999':
		return None
	elif value in ['0000']:
		# this typically means "open" (at beginning)
		return None

	# First try dateutil's parser
	try:
		begin = parser.parse(value, default=default_dt)
		dt2 = dp_parser.get_date_data(value)
		# dateparser works very badly for text
		# but does do a good job finding the precision
		if dt2.date_obj:
			if dt2.period == 'year':
				end = begin.replace(year=begin.year+1)
			elif dt2.period == 'month':
				if begin.month == 12:
					end = begin.replace(year=begin.year+1, month=1)
				else:
					end = begin.replace(month=begin.month+1)
			elif dt2.period == 'day':
				end = begin + timedelta(days=1)
		else:
			# print(f"Failed to parse with dateparser: {value} / {begin}")
			# assuming year
			if begin.day != 1:
				end = begin + timedelta(days=1)
			elif begin.month != 1:
				if begin.month == 12:
					end = begin.replace(year=begin.year+1, month=1)
				else:
					end = begin.replace(month=begin.month+1)
			else:
				end = begin.replace(year=begin.year+1)

	except parser.ParserError: 
		# Nope, try the edtf parser

		# 19XX or 19?? --> 19xx
		# These break text_to_edtf, so catch early
		if len(value) == 5 and value[4] == '?':
			value = value[:4] + '~'
		if '?' in value or 'X' in value:
			value = x_re.sub('\g<1>u', value)
			value = x_re.sub('\g<1>u', value)
		value = value.replace('-00', '-uu')

		try:
			# Could be actual EDTF but not parsable by dateutils
			dt = parse_edtf(value)
			if type(dt) in [UOA, PUOA, MP]:
				begin = dt.lower_fuzzy()
				end = dt.upper_fuzzy()
			else:
				begin = dt.lower_strict()
				end = dt.upper_strict()			
			try:
				begin = struct_time_to_datetime(begin)
				end = struct_time_to_datetime(end)
			except:
				# BCE dates
				begstr = time.strftime('%Y-%m-%dT%H:%M:%SZ', begin)
				# FIXME -- this should be -1 seconds :/
				endstr = time.strftime('%Y-%m-%dT%H:%M:%SZ', end)
				return (begstr, endstr)
		except:
			# Nope, try text to edtf
			try:
				v2 = text_to_edtf(value)
				if v2:
					value = v2
			except:
				pass
			try:
				dt = parse_edtf(value)
				if type(dt) in [UOA, PUOA, MP]:
					begin = dt.lower_fuzzy()
					end = dt.upper_fuzzy()
				else:
					begin = dt.lower_strict()
					end = dt.upper_strict()
				try:
					begin = struct_time_to_datetime(begin)
					end = struct_time_to_datetime(end)
				except:
					# BCE dates
					begstr = time.strftime('%Y-%m-%dT%H:%M:%SZ', begin)
					# FIXME -- this should be -1 seconds :/
					endstr = time.strftime('%Y-%m-%dT%H:%M:%SZ', end)
					return (begstr, endstr)
			except:
				# last attempt with dateparser for other locales
				try:
					dt2 = dp_parser.get_date_data(value)
					if dt2.period == 'day' and dt2.locale != 'en':
						begin = dt2.date_obj
						end = begin + timedelta(days=1)
					elif dt2:
						print(f"dateparser found: {dt2} from {value} ?")
						return None
					else:
						print(f"Failed to parse date: {value}")
						return None
				except:
					print(f"Failed to parse date: {value}")
					return None

	# CRM utter stupidity
	end = end - timedelta(seconds=1)
	return (begin.isoformat()+"Z", end.isoformat()+"Z")

