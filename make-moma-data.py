from cromulent import model, vocab
from csv import DictReader

from pipeline.sources.mapper_utils import make_datetime

# configure model.factory here

vocab.set_linked_art_uri_segments()
vocab.add_linked_art_boundary_check()
model.factory.auto_assign_id = False
model.factory.json_serializer = "fast"
model.factory.base_url = "https://www.moma.org/data/"
model.factory.base_dir = "updated-moma-transform"


la_institutions = {}
inst_x = 1
with open('scripts/MoMA-exhibitors.csv') as csvfh:
    reader = DictReader(csvfh, fieldnames=['name', 'ulan', 'wikidata'], dialect="excel")
    for row in reader:
        if row['wikidata'] != 'Wikidata':
            # not a header
            inst = model.Group(ident=f"inst{inst_x}", label=f"{row['name']}")
            inst_x += 1
            inst.identified_by = model.Name(content=row['name'])
            if row['ulan']:
                inst.equivalent = model.Group(ident=f"http://vocab.getty.edu/ulan/{row['ulan']}", label=f"{row['name']} - ULAN")
            if row['wikidata']:
                inst.equivalent = model.Group(ident=f"http://www.wikidata.org/entity/{row['wikidata']}", label=f"{row['name']} - Wikidata")
            la_institutions[row['name']] = inst

fields = ['Unique_ExhibitionsEvents_ID',	'ExhibitingInstitution QID',	'ExhibitingInstitution',	'Exhibition Title',	\
    'Startdate',	'EndDate',	'MoMA_TMS_Exhibition_ID',	'Exhibitions_Events_URL',	'location',	'Venue', \
        	 "VenueQID"	, "Street_address", "City", "State", "Zipcode", "Lat", "Long", "Constituent Role"	, "Unique_Constituents_ID", \
 "MoMA_TMS_Constituents_ID", "ConstituentType", "AlphaSortName", "DisplayName", "Institution", "Nationality", "TMSBirthYear", \
 "TMSDeathYear", "TMS", "DisplayBio", "Gender", "VIAFID", "WikidataQID", "ULANID"]


la_artists = {}
la_exhibitions = {}
missed_natls = {}

aat_male = vocab.instances['male']
aat_female = vocab.instances['female']

vocab.instances['new zealander nationality'] = vocab.Nationality(ident="http://vocab.getty.edu/aat/300021959")
vocab.instances['brazilian nationality'] = vocab.Nationality(ident="http://vocab.getty.edu/aat/300107967")
# finnish
# south african
# israeli
# argentine
# chilean 


carried_out_roles = ['Curator', 'Guest Curator', 'Organizer', 'Designer', 'PartnerOrg', \
    'Director', 'Assistant Curator', 'Selector', 'Installer', 'Assembler', 'Arranger', \
    'Preparer', 'Advisor', 'Competition Judge', 'Supervisor']

with open('scripts/moma-alternative-exhibitions-with-locations.csv') as csvfh:
    reader = DictReader(csvfh, fieldnames=fields, dialect="excel")
    for row in reader:

        if row['Unique_ExhibitionsEvents_ID'] != 'Unique_ExhibitionsEvents_ID':
               
                
            artist_id = row['Unique_Constituents_ID']
            if artist_id in la_artists:
                artist = la_artists[artist_id]
            elif not artist_id:
                artist = None
            else:
                # Build it
                if row['ConstituentType'] == 'Individual':
                    artist_class = model.Person
                elif row['ConstituentType'] in ['Institution', 'Unknown or Various']:
                    artist_class = model.Group
                else:
                    print("Unknown type: " + row['ConstituentType'])
                    raise ValueError()
                artist = artist_class(ident=artist_id)
                # Attach MoMA identifier
                if row['DisplayName']:
                    artist.identified_by = model.Name(content=row['DisplayName'], label='DisplayName')

                    artist._label =  content=row['DisplayName']
                # add alphasortname
                if row['AlphaSortName']:
                    artist.identified_by = model.Name(content=row['AlphaSortName'], label='AlphaSortName')
                   
                if row['MoMA_TMS_Constituents_ID']:
                    mid = model.Identifier(content=row['MoMA_TMS_Constituents_ID'])
                    aa = model.AttributeAssignment()
                    aa.carried_out_by = la_institutions['The Metropolitan Museum of Art']
                    mid.attributed_by = aa
                    artist.identified_by = mid
                if row['ULANID']:
                    artist.equivalent = artist_class(ident=f"http://vocab.getty.edu/ulan/{row['ULANID']}", label=f"{row['DisplayName']} - ULAN")
                if row['WikidataQID']:
                    artist.equivalent = artist_class(ident=f"http://www.wikidata.org/entity/{row['WikidataQID']}", label=f"{row['DisplayName']} - Wikidata")
                if row['VIAFID']:
                    artist.equivalent = artist_class(ident=f"http://viaf.org/viaf/{row['VIAFID']}", label=f"{row['DisplayName']} - VIAF")


                if row['Gender'] in ["Male", 'male']:
                    artist.classified_as = aat_male
                elif row['Gender'] in ["Female", 'female']:
                    artist.classified_as = aat_female
                elif row['Gender']:
                    #t = 1
                    print(f"Missing gender: {row['Gender']}")
                    #raise ValueError


                if row['Nationality']:
                    natl = row['Nationality'].lower()
                    if f"{natl} nationality" in vocab.instances:
                        artist.classified_as = vocab.instances[f"{natl} nationality"]
                    elif natl == "nationality unknown":
                        # = no data
                        pass
                    elif " and " in natl or "/" in natl:
                        # multiple
                        pass
                    elif row['ULANID']:   
                        # print(f"Skipping nationality, just get it from ULAN {row['ULANID']}: {row['Nationality']}")
                        pass
                    else:
                        try:
                            missed_natls[natl] += 1
                        except:
                            print(f"Missed nationality: {natl}")
                            missed_natls[natl] = 1

                if row['DisplayBio']:
                    artist.referred_to_by = vocab.BiographyStatement(content=row['DisplayBio'])

                if row['TMSBirthYear']:
                    if artist_class == model.Person:
                        b = model.Birth()
                        artist.born = b
                    else:
                        b = model.Formation()
                        artist.formed_by = b
                    b.timespan = model.TimeSpan()
                    b.timespan.begin_of_the_begin = f"{row['TMSBirthYear']}-01-01T00:00:00Z"
                    b.timespan.end_of_the_end = f"{row['TMSBirthYear']}-12-31T23:59:59Z"
                    b.timespan.identified_by = vocab.DisplayName(content=row['TMSBirthYear'])

                if row['TMSDeathYear']:
                    if artist_class == model.Person:
                        d = model.Death()
                        artist.died = d
                    else:
                        d = model.Dissolution()
                        artist.dissolved_by = d
                    d.timespan = model.TimeSpan()
                    d.timespan.begin_of_the_begin = f"{row['TMSDeathYear']}-01-01T00:00:00Z"
                    d.timespan.end_of_the_end = f"{row['TMSDeathYear']}-12-31T23:59:59Z"
                    d.timespan.identified_by = vocab.DisplayName(content=row['TMSDeathYear'])

                la_artists[artist_id] = artist


            # Now Exhibition Activity and Concept

            exh_id = row['Unique_ExhibitionsEvents_ID']
            if exh_id in la_exhibitions:
                exh_act = la_exhibitions[exh_id]
                # exh_concept = exh_act.motivated_by[0]
            else:

                
                # Build it
                exh_act = vocab.Exhibition(ident=exh_id)
                #exh_concept = model.InformationObject(ident=exh_id)
                #exh_act.motivated_by = exh_concept
                if row['Exhibition Title']:
                    exh_act.identified_by = vocab.PrimaryName(content=row['Exhibition Title'])
                    exh_act._label = content=row['Exhibition Title']
                if row['ExhibitingInstitution']:
                    inst = la_institutions.get(row['ExhibitingInstitution'], None)
                    if not inst:
                        print(f"Missing institution: {row['ExhibitingInstitution']}")
                        raise ValueError()
                    exh_act.carried_out_by = inst

                if row['Venue']:
                    # venue name with geocoords
                    place = vocab.Place(label= row['Venue'])
                    place.defined_by = "POINT(" + row['Lat'] + ' ' +  row['Long'] + ")"
                    place.classified_as = model.Type(ident="http://vocab.getty.edu/aat/300260522", label="exhibition building spaces")
                    exh_act.took_place_at = place

                    # street address
                    place2 = vocab.Place(label=row["Street_address"])
                    place2.classified_as = model.Type(ident="http://vocab.getty.edu/aat/300419273", label="thoroughfare names")
                    exh_act.took_place_at = place2

                # timespan
                ts = model.TimeSpan()
                if row['Startdate']:    
                    # ... US date formatted string ...
                    try:
                        (begin, end) = make_datetime(row['Startdate'])
                        ts.begin_of_the_begin = begin
                    except:
                        print(f"Unparseable date: {row['Startdate']}")
                        exit
                if row['EndDate']:
                    # ... Another US date formatted string ...
                    try:
                        (begin, end) = make_datetime(row['EndDate'])
                        ts.end_of_the_end = end
                    except:
                        print(f"Unparseable date: {row['EndDate']}")
                exh_act.timespan = ts

                if row['Exhibitions_Events_URL']:
                    web_lo = model.LinguisticObject()
                    web_do = vocab.WebPage()
                    web_do.access_point = model.DigitalObject(ident=row['Exhibitions_Events_URL'])
                    web_lo.digitally_carried_by = web_do
                    exh_act.subject_of = web_lo

            if artist:
                if row['Constituent Role'] == 'Artist':
                    exh_act.influenced_by = artist
                elif row['Constituent Role'] in carried_out_roles:
                    exh_act.carried_out_by = artist
                elif not row['Constituent Role']:
                    # ... nothing to do ...
                    pass
                else:
                    pass
                    #print(f"Unknown relationship between constituent and exhibition: {row['Constituent Role']}")

            la_exhibitions[exh_id] = exh_act
# print(missed_natls)

for scope in [la_institutions, la_artists, la_exhibitions]:
    for v in scope.values():
        model.factory.toFile(v)
