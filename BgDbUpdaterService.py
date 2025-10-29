import mysql.connector

from mysql.connector import Error
from urllib.request import urlopen
from urllib.request import Request
from xml.etree.ElementTree import parse
import ssl
import time
from datetime import datetime
from flask import current_app


def get_game_data_key_from_data_type(i):
    switcher = {
        "boardgamecategory": "categories",
        "boardgamemechanic": "mechanics",
        "boardgamedesigner": "designers",
        "category": "categories",
        "mechanic": "mechanics",
        "designer": "designers"
    }
    return switcher.get(i, "")


def add_additional_data(link_type, game_data, xml_item):
    """ Add additional game data found in link elements where type='link_type'  """

    values = []
    for item in xml_item.findall('.//link[@type="'+link_type+'"]'):
        values.append(item.attrib['value'])
    game_data[get_game_data_key_from_data_type(link_type)] = values
    return game_data


def get_polled_min_age(age_poll):
    """ Get minimum age from polled BGG users """

    polled_min_age = 0
    votes = 0
    for vote in age_poll:
        if int(vote.attrib['numvotes']) > votes:
            votes = int(vote.attrib['numvotes'])
            polled_min_age = int(vote.attrib['value'])
    return polled_min_age


def get_query_for_game_update(bgg_id, sql_insert_values, name_locked):
    """ Query Builder for game values """

    query_update = "UPDATE game SET"

    column_value_pairs = []
    for column in sql_insert_values:
        if isinstance(sql_insert_values[column], float):
            if column == "weight":
                column_value_pairs.append("%s = %.2f" % (column, round(sql_insert_values[column], 2)) )
            else:
                column_value_pairs.append("%s = %.1f" % (column, round(sql_insert_values[column], 2)) )
        elif isinstance(sql_insert_values[column], int):
            column_value_pairs.append("%s = %d" % (column, sql_insert_values[column]))
        elif isinstance(sql_insert_values[column], str):
            if not name_locked and column != "name":
                column_value_pairs.append("%s = '%s'" % (column, sql_insert_values[column]))
        elif isinstance(sql_insert_values[column], datetime):
            column_value_pairs.append("%s = '%s'" % (column, sql_insert_values[column]))

    query_values = ", ".join(column_value_pairs)

    query_where_clause = "WHERE bgg_id = %d" % bgg_id

    return "%s %s %s" % (query_update, query_values, query_where_clause)


def get_query_for_game_expansion_update(bgg_id, sql_insert_values):
    """ Query Builder for linking base games to their expansion values """

    if sql_insert_values["stage"] == "base":
        query_insert = "INSERT IGNORE INTO game_expansion (expansion_id, game_id) VALUES "
        queries = []
        if len(sql_insert_values["game_expansions"]) > 0:
            for expansion_id in sql_insert_values["game_expansions"]:
                # queries.append(query_template % (bgg_id, expansion_id))
                queries.append("(%d, %d)" % (expansion_id, bgg_id))
            return query_insert + ",\n".join(queries)
        else:
            return ""


# SETUP METHODS
def connect():
    """ Connect to MySQL database """
    connection = None
    try:
        connection = mysql.connector.connect(**current_app.config['DATASOURCE'])
        if connection.is_connected():
            print('Connected to boardgamedb')
        connection.raise_on_warnings = False
    except Error as e:
        print(e)

    return connection


def disconnect(connection):
    """ Disconnect from MySQL database """

    if connection is not None and connection.is_connected():
        connection.close()
        print('Disconnected from boardgamedb')
    else:
        print('Nothing to Disconnect')


def get_ssl_context():
    """ Get SSL context to ignore Cert Validation"""

    myssl = ssl.create_default_context()
    myssl.check_hostname = False
    myssl.verify_mode = ssl.CERT_NONE
    return myssl


def add_game_data_from_xml(game_data, xml_item):
    """ Add basic game details to game data """

    game_data.update({
        'name': xml_item.find('./name').attrib['value'].replace("'", "''"),
        'year_published': int(xml_item.find('./yearpublished').attrib['value']),
        'min_age': int(xml_item.find('./minage').attrib['value']),
        'min_players': int(xml_item.find('./minplayers').attrib['value']),
        'max_players': int(xml_item.find('./maxplayers').attrib['value']),
        'min_time': int(xml_item.find('./minplaytime').attrib['value']),
        'max_time': int(xml_item.find('./maxplaytime').attrib['value']),
        'description': xml_item.findtext('description').replace("'", "''"),
        'image_url': xml_item.findtext('thumbnail'),
        'bgg_rating': float((xml_item.find('./statistics/ratings/bayesaverage').attrib['value'])),
        'user_rating': float((xml_item.find('./statistics/ratings/average').attrib['value'])),
        'weight': float((xml_item.find('./statistics/ratings/averageweight').attrib['value'])),
        'last_bgg_check': datetime.now()
    })

    age_poll_results = xml_item.find('.//poll[@name="suggested_playerage"]').find('results')
    if age_poll_results is not None:
        age_poll = age_poll_results.findall('result') if age_poll_results is not None else None
        game_data['polled_min_age'] = get_polled_min_age(age_poll) if age_poll is not None else 0

    return game_data


class BgDbUpdaterService:

    TEST_LIST_OF_TWO = [420120]
    BGG_API2_URL = 'https://boardgamegeek.com/xmlapi2/thing?id=%s&stats=1'
    TIME_DELAY = 2.1
    TIME_BETWEEN_UPDATES = 24 * 60 * 60  # 1 day
    ALL_BGG_IDS = []
    DEBUG = False
    SSL_CONTEXT = None
    CONNECTION = None
    AUTH_STRING = None

    NEW_VALUES = {
        "category": set(),
        "mechanic": set(),
        "designer": set(),
    }

    def __init__(self, config):
        self.CONNECTION = connect()
        self.SSL_CONTEXT = get_ssl_context()
        self.AUTH_STRING = config['BEARER_TOKEN_STRING']

    # BGG API DATA REQUESTS & PROCESSING
    def get_raw_xml_from_bgg_api(self, bgg_id):
        headers = {'Authorization': self.AUTH_STRING}
        req = Request(self.BGG_API2_URL % bgg_id, headers=headers)
        with urlopen(req,  context=self.SSL_CONTEXT) as response:
            raw_xml = parse(response)
            return raw_xml

    def add_game_expansions(self, game_data, xml_item):
        """ Add expansions to game data """

        if game_data["stage"] == "base":
            expansion_ids = []
            for expansion in xml_item.findall('.//link[@type="boardgameexpansion"]'):
                if int(expansion.attrib['id']) in self.ALL_BGG_IDS:
                    expansion_ids.append(int(expansion.attrib['id']))
            game_data["game_expansions"] = expansion_ids
        return game_data

    def get_data_for_sql(self, bgg_id):
        """ Process xml from Boardgame Geek API into data map """

        xml_doc = self.get_raw_xml_from_bgg_api(bgg_id)

        if not xml_doc.find('./item'):
            print("bgg_id not found on BoardGameGeek.")
            return None

        game_type = xml_doc.find('./item').attrib['type']
        game_data = {'stage': 'expansion' if game_type == 'boardgameexpansion' else 'base'}

        for xml_item in xml_doc.iterfind('item'):
            game_data = add_game_data_from_xml(game_data, xml_item)
            game_data = self.add_game_expansions(game_data, xml_item)
            game_data = add_additional_data("boardgamecategory", game_data, xml_item)
            game_data = add_additional_data("boardgamemechanic", game_data, xml_item)
            game_data = add_additional_data("boardgamedesigner", game_data, xml_item)

        return game_data

    # HELPER METHODS

    def get_queries_for_addition_data_type_update(self, data_type, bgg_id, game_data, existing_value_id_map):
        """ Query Builder for linking base games to their additional data in the correlation table,
        but also add that data to it's unique table if doesn't exist """

        if data_type not in ["category", "mechanic", "designer"]:
            return "", "", ""

        new_values_to_create = []
        existing_value_ids = []
        new_game_value_insert_statements = []
        for value in game_data[get_game_data_key_from_data_type(data_type)]:
            if value not in existing_value_id_map:
                quote_fixed_value = value.replace("'", "''")
                if value not in self.NEW_VALUES[data_type]:
                    new_values_to_create.append(quote_fixed_value)
                    self.NEW_VALUES[data_type].add(value)
                query_select_template = "(%d, (SELECT id FROM "+data_type+" WHERE name = '%s'))"
                new_game_value_insert_statements.append(query_select_template % (bgg_id, quote_fixed_value))
            else:
                existing_value_ids.append(existing_value_id_map[value])

        value_update_queries = ""
        new_game_value_update_queries = ""
        if len(new_values_to_create) > 0:
            value_update_queries = "INSERT IGNORE INTO "+data_type+" (name) VALUES \n('" \
                                      + "'),\n('".join(new_values_to_create) + "');"

            new_game_value_update_queries = "INSERT INTO game_"+data_type+" (bgg_id, "+data_type+"_id) VALUES \n" \
                                            + ",\n".join(new_game_value_insert_statements) + ";"

        game_value_update_queries = ""
        if len(existing_value_ids) > 0:
            game_value_update_queries_template = \
                "INSERT IGNORE INTO game_"+data_type+" (bgg_id, "+data_type+"_id) VALUES \n(%d, " \
                + ("),\n(" + str(bgg_id) + ", ").join(str(value_id) for value_id in existing_value_ids) + ");"
            game_value_update_queries = game_value_update_queries_template % bgg_id

        # print(""+data_type+"_update_queries: \n" + value_update_queries)
        # print("new_game_"+data_type+"_update_queries: \n" + new_game_value_update_queries)
        # print("game_"+data_type+"_update_queries: \n" + game_value_update_queries)
        return value_update_queries, new_game_value_update_queries, game_value_update_queries

    # CRUD METHODS
    def get_bgg_ids(self, skip_recently_modified):
        """ Retrieve BGG ids for all games """

        query = "SELECT bgg_id, last_bgg_check FROM game"

        cursor = self.CONNECTION.cursor()
        print("...Retrieving games to update...")
        cursor.execute(query)
        games_to_update = []
        for row in cursor:
            if row[0] < 10000000:
                # print(row[0])
                self.ALL_BGG_IDS.append(row[0])  # global constant needed for other queries.
                # row[1] is last modified date
                if row[1]:
                    time_since_last_update = (datetime.now()-(row[1])).total_seconds()
                if skip_recently_modified and time_since_last_update > self.TIME_BETWEEN_UPDATES:
                    games_to_update.append(row[0])

        return games_to_update if skip_recently_modified else self.ALL_BGG_IDS

    def get_bgg_ids_for_new_games(self):
        """ Retrieve BGG ids for games that don't have a value for name """
        query = "SELECT bgg_id FROM game WHERE stage IS NULL"

        cursor = self.CONNECTION.cursor()
        print("...Retrieving games to update...")
        cursor.execute(query)
        games_to_update = []
        for row in cursor:
            games_to_update.append(row[0])

        return games_to_update

    def get_existing_category_id_map(self):
        """ Retrieve map of category ids """

        query = "SELECT name, id FROM category"

        cursor = self.CONNECTION.cursor()
        existing_category_id_map = {}
        cursor.execute(query)
        for row in cursor:
            existing_category_id_map[row[0]] = int(row[1])

        return existing_category_id_map

    def get_existing_value_id_map(self, data_type):
        """ Retrieve map of additional value ids """
        query = "SELECT name, id FROM " + data_type

        cursor = self.CONNECTION.cursor()
        existing_value_id_map = {}
        cursor.execute(query)
        for row in cursor:
            existing_value_id_map[row[0]] = int(row[1])

        return existing_value_id_map

    def update_games(self, bgg_ids):
        """ Update Games fully or partially"""

        if len(bgg_ids) == 0:
            print("=================")
            print("Nothing to Update")
            print("=================")
            return

        try:
            existing_category_id_map = self.get_existing_value_id_map("category")
            existing_mechanic_id_map = self.get_existing_value_id_map("mechanic")
            existing_designer_id_map = self.get_existing_value_id_map("designer")
        except Error as e:
            print(e)
            existing_category_id_map = {}
            existing_mechanic_id_map = {}
            existing_designer_id_map = {}

        successful_game_updates = []
        failures = []

        print("\nAttempting to update %d games" % len(bgg_ids))
        time_estimate = len(bgg_ids)*self.TIME_DELAY
        self.set_db_config_to_locked()
        print("This should take approx. %d seconds" % time_estimate)
        print("===========================================================")

        count = 0
        for bgg_id in bgg_ids:
            count += 1
            print("Attempting to update bgg_id %s...(%d/%d)....." % (bgg_id, count, len(bgg_ids)), end="")
            game_data = self.get_data_for_sql(bgg_id)
            if not game_data:
                continue
            time.sleep(self.TIME_DELAY)

            name_locked = self.check_name_locked(bgg_id)

            category_query, new_game_category_query, game_category_query = \
                self.get_queries_for_addition_data_type_update('category', bgg_id, game_data, existing_category_id_map)
            mechanic_query, new_game_mechanic_query, game_mechanic_query = \
                self.get_queries_for_addition_data_type_update('mechanic', bgg_id, game_data, existing_mechanic_id_map)
            designer_query, new_game_designer_query, game_designer_query = \
                self.get_queries_for_addition_data_type_update('designer', bgg_id, game_data, existing_designer_id_map)
            try:
                if not self.DEBUG:
                    cursor = self.CONNECTION.cursor()

                    cursor.execute(get_query_for_game_update(bgg_id, game_data, name_locked))
                    cursor.execute(get_query_for_game_expansion_update(bgg_id, game_data))

                    cursor.execute(category_query)
                    cursor.execute(new_game_category_query)
                    cursor.execute(game_category_query)

                    cursor.execute(mechanic_query)
                    cursor.execute(new_game_mechanic_query)
                    cursor.execute(game_mechanic_query)

                    cursor.execute(designer_query)
                    cursor.execute(new_game_designer_query)
                    cursor.execute(game_designer_query)

                    self.CONNECTION.commit()
                    cursor.close()
                    successful_game_updates.append(bgg_id)
                print("SUCCESS\n\n")
            except Error as e:
                try:
                    print("MySQL Error [%d]: %s" % (e.args[0], e.args[1]))
                except IndexError:
                    print("MySQL Error: %s" % str(e))
                finally:
                    failures.append(bgg_id)
            except TypeError as e:
                print("FAILED")
                print(e)
                failures.append(bgg_id)
            except ValueError as e:
                print("FAILED")
                print(e)
                failures.append(bgg_id)

        print("===========================================================")
        if len(successful_game_updates) > 0:
            print("   Successfully updated %d games of %d attempted" % (len(successful_game_updates), len(bgg_ids)))
        if len(failures) > 0:
            print("   Failed to update %d games: [%s]" % (len(failures), ", ".join(str(failures))))
        print("===========================================================")

    def update_lock_status(self, lock_status):
        query = "UPDATE configuration SET value = '%s' WHERE type = 'update_lock'" % lock_status
        cursor = self.CONNECTION.cursor()
        cursor.execute(query)
        self.CONNECTION.commit()
        cursor.close()

    def set_db_config_to_locked(self):
        print("Locking Database")
        self.update_lock_status("LOCKED")

    def set_db_config_to_unlocked(self):
        print("Unlocking Database")
        self.update_lock_status("UNLOCKED")

    def get_lock_status(self):
        query = "SELECT value FROM configuration WHERE type = 'update_lock'"
        cursor = self.CONNECTION.cursor()
        cursor.execute(query)
        lock_status = cursor.fetchone()[0]
        cursor.close()
        return True if str(lock_status).upper() == 'LOCKED' else False

    # API METHODS
    def update_all_games(self, skip_recently_modified):
        self.set_db_config_to_locked()
        self.update_games(self.get_bgg_ids(skip_recently_modified))
        self.set_db_config_to_unlocked()
        disconnect(self.CONNECTION)

    def update_new_games(self):
        self.set_db_config_to_locked()
        self.update_games(self.get_bgg_ids_for_new_games())
        self.set_db_config_to_unlocked()
        disconnect(self.CONNECTION)

    def test_update(self):
        self.set_db_config_to_locked()
        self.update_games(self.TEST_LIST_OF_TWO)
        self.set_db_config_to_unlocked()
        disconnect(self.CONNECTION)

    def lock_database(self):
        self.set_db_config_to_locked()
        disconnect(self.CONNECTION)

    def unlock_database(self):
        self.set_db_config_to_unlocked()
        disconnect(self.CONNECTION)

    def check_name_locked(self, bgg_id):
        query = "SELECT lock_title FROM game WHERE bgg_id = " + str(bgg_id)
        cursor = self.CONNECTION.cursor()
        cursor.execute(query)
        name_locked = bool(cursor.fetchone()[0])
        return name_locked