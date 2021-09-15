import mysql.connector
from mysql.connector import Error
from urllib.request import urlopen
from xml.etree.ElementTree import parse
import ssl
import time
from datetime import datetime
from configurations import CONFIG


TEST_LIST_OF_ONE = [202102]
TEST_LIST_OF_TWO = [3837, 3837]
BGG_API_URL = 'https://www.boardgamegeek.com/xmlapi/boardgame/%s'
BGG_API2_URL = 'https://www.boardgamegeek.com/xmlapi2/thing?id=%s&stats=1'
TIME_DELAY = 2.1
TIME_BETWEEN_UPDATES = 24*60*60  # 1 day
ALL_BGG_IDS = []
DEBUG = False
SSL_CONTEXT = None
CONNECTION = None
NEW_VALUES = {
    "category": set(),
    "mechanic": set(),
    "designer": set(),
}


# SETUP METHODS
def connect():
    """ Connect to MySQL database """
    connection = None
    try:
        connection = mysql.connector.connect(**CONFIG)
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


# BGG API DATA REQUESTS & PROCESSING
def get_raw_xml_from_bgg_api(bgg_id):
    response = urlopen(BGG_API2_URL % bgg_id, context=SSL_CONTEXT)
    return parse(response)


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
        'last_bgg_check': datetime.now()
    })

    age_poll_results = xml_item.find('.//poll[@name="suggested_playerage"]').find('results')
    if age_poll_results is not None:
        age_poll = age_poll_results.findall('result') if age_poll_results is not None else None
        game_data['polled_min_age'] = get_polled_min_age(age_poll) if age_poll is not None else 0

    return game_data


def add_game_expansions(game_data, xml_item):
    """ Add expansions to game data """

    if game_data["stage"] == "base":
        expansion_ids = []
        for expansion in xml_item.findall('.//link[@type="boardgameexpansion"]'):
            if int(expansion.attrib['id']) in ALL_BGG_IDS:
                expansion_ids.append(int(expansion.attrib['id']))
        game_data["game_expansions"] = expansion_ids
    return game_data


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


def get_data_for_sql(bgg_id):
    """ Process xml from Boardgame Geek API into data map """

    xml_doc = get_raw_xml_from_bgg_api(bgg_id)

    game_type = xml_doc.find('./item').attrib['type']
    game_data = {'stage': 'expansion' if game_type == 'boardgameexpansion' else 'base'}

    for xml_item in xml_doc.iterfind('item'):
        game_data = add_game_data_from_xml(game_data, xml_item)
        game_data = add_game_expansions(game_data, xml_item)
        game_data = add_additional_data("boardgamecategory", game_data, xml_item)
        game_data = add_additional_data("boardgamemechanic", game_data, xml_item)
        game_data = add_additional_data("boardgamedesigner", game_data, xml_item)

    return game_data


# HELPER METHODS
def get_polled_min_age(age_poll):
    """ Get minimum age from polled BGG users """

    polled_min_age = 0
    votes = 0
    for vote in age_poll:
        if int(vote.attrib['numvotes']) > votes:
            votes = int(vote.attrib['numvotes'])
            polled_min_age = int(vote.attrib['value'])
    return polled_min_age


def get_query_for_game_update(bgg_id, sql_insert_values):
    """ Query Builder for game values """

    query_update = "UPDATE game SET"

    column_value_pairs = []
    for column in sql_insert_values:
        if isinstance(sql_insert_values[column], float):
            column_value_pairs.append("%s = %.1f" % (column, sql_insert_values[column]))
        elif isinstance(sql_insert_values[column], int):
            column_value_pairs.append("%s = %d" % (column, sql_insert_values[column]))
        elif isinstance(sql_insert_values[column], str) or isinstance(sql_insert_values[column], datetime):
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


def get_queries_for_addition_data_type_update(data_type, bgg_id, game_data, existing_value_id_map):
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
            if value not in NEW_VALUES[data_type]:
                new_values_to_create.append(quote_fixed_value)
                NEW_VALUES[data_type].add(value)
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
def get_bgg_ids(skip_recently_modified):
    """ Retrieve BGG ids for all games """

    query = "SELECT bgg_id, last_bgg_check FROM game"

    cursor = CONNECTION.cursor()
    print("...Retrieving games to update...")
    cursor.execute(query)
    games_to_update = []
    for row in cursor:
        if row[0] < 10000000:
            print(row[0])
            ALL_BGG_IDS.append(row[0])  # global constant needed for other queries.
            if row[1]:
                time_since_last_update = (datetime.now()-(row[1])).total_seconds()
            if skip_recently_modified and time_since_last_update > TIME_BETWEEN_UPDATES:  # row[] is last modified date
                games_to_update.append(row[0])

    return games_to_update if skip_recently_modified else ALL_BGG_IDS


def get_bgg_ids_for_new_games():
    """ Retrieve BGG ids for games that don't have a value for name """
    query = "SELECT bgg_id FROM game WHERE stage IS NULL"

    cursor = CONNECTION.cursor()
    print("...Retrieving games to update...")
    cursor.execute(query)
    games_to_update = []
    for row in cursor:
        games_to_update.append(row[0])

    return games_to_update


def get_existing_category_id_map():
    """ Retrieve map of category ids """

    query = "SELECT name, id FROM category"

    cursor = CONNECTION.cursor()
    existing_category_id_map = {}
    cursor.execute(query)
    for row in cursor:
        existing_category_id_map[row[0]] = int(row[1])

    return existing_category_id_map


def get_existing_value_id_map(data_type):
    """ Retrieve map of additional value ids """
    query = "SELECT name, id FROM " + data_type

    cursor = CONNECTION.cursor()
    existing_value_id_map = {}
    cursor.execute(query)
    for row in cursor:
        existing_value_id_map[row[0]] = int(row[1])

    return existing_value_id_map


def update_games(bgg_ids):
    """ Update Games fully or partially"""

    if len(bgg_ids) == 0:
        print("=================")
        print("Nothing to Update")
        print("=================")
        return

    try:
        existing_category_id_map = get_existing_value_id_map("category")
        existing_mechanic_id_map = get_existing_value_id_map("mechanic")
        existing_designer_id_map = get_existing_value_id_map("designer")
    except Error as e:
        print(e)
        existing_category_id_map = {}
        existing_mechanic_id_map = {}
        existing_designer_id_map = {}

    successful_game_updates = []
    failures = []

    print("\nAttempting to update %d games" % len(bgg_ids))
    time_estimate = len(bgg_ids)*TIME_DELAY
    print("This should take approx. %d seconds" % time_estimate)
    print("===========================================================")

    count = 0
    for bgg_id in bgg_ids:
        count += 1
        print("Attempting to update bgg_id %s...(%d/%d)....." % (bgg_id, count, len(bgg_ids)), end="")
        game_data = get_data_for_sql(bgg_id)
        time.sleep(TIME_DELAY)

        category_query, new_game_category_query, game_category_query = \
            get_queries_for_addition_data_type_update('category', bgg_id, game_data, existing_category_id_map)
        mechanic_query, new_game_mechanic_query, game_mechanic_query = \
            get_queries_for_addition_data_type_update('mechanic', bgg_id, game_data, existing_mechanic_id_map)
        designer_query, new_game_designer_query, game_designer_query = \
            get_queries_for_addition_data_type_update('designer', bgg_id, game_data, existing_designer_id_map)
        try:
            if not DEBUG:
                cursor = CONNECTION.cursor()

                cursor.execute(get_query_for_game_update(bgg_id, game_data))
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

                CONNECTION.commit()
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
        print("   Successfully updated %d games" % len(successful_game_updates))
    if len(failures) > 0:
        print("   Failed to update %d games: [%s]" % (len(failures), ", ".join(str(failures))))
    print("===========================================================")

    disconnect(CONNECTION)


# CORE METHODS
def update_all_games(skip_recently_modified):
    update_games(get_bgg_ids(skip_recently_modified))


def update_new_games():
    update_games(get_bgg_ids_for_new_games())


def test_update():
    update_games(TEST_LIST_OF_TWO)


if __name__ == '__main__':
    CONNECTION = connect()
    SSL_CONTEXT = get_ssl_context()
    # update_all_games(skip_recently_modified=True)
    update_new_games()
    # test_update()
