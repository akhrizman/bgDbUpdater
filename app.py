from flask import Flask, jsonify, request, json
from BgDbUpdaterService import BgDbUpdaterService
import os

_deployed_env_ = os.environ.get("FLASK_ENV", default=None)

# Flask app init
app = Flask(__name__)   # Flask constructor

# read from initial config in settings.py
app.config.from_object('settings')


# override env variables based on deploy target
if _deployed_env_ is not None:
    if _deployed_env_ == 'local':
        app.config.from_pyfile('./appconfigs/dev_settings.py')
    elif _deployed_env_ == 'development':
        app.config.from_pyfile('./appconfigs/dev_settings.py')
    elif _deployed_env_ == 'production':
        app.config.from_pyfile('./appconfigs/prod_settings.py')
    else:
        raise RuntimeError('Unknown environment setting provided.')


@app.route('/')
def hello():
    return {'environment': _deployed_env_,
            'database': app.config['DATASOURCE']['database']}


@app.route('/syncAll', methods=['POST'])
def sync_all_games():
    print('Attempting to Update All Games')
    bg_updater_service = BgDbUpdaterService()
    locked = bg_updater_service.get_lock_status()

    if locked:
        return app.response_class(json.dumps('Database is Locked. Try again later'), content_type='application/json')

    bg_updater_service.update_all_games(skip_recently_modified=True)
    return app.response_class(json.dumps('Sync Complete'), content_type='application/json')


@app.route('/syncAllIncludingRecent', methods=['POST'])
def sync_all_games_including_recent():
    print('Attempting to Update All Games including recently updated')
    bg_updater_service = BgDbUpdaterService()
    locked = bg_updater_service.get_lock_status()

    if locked:
        return app.response_class(json.dumps('Database is Locked. Try again later'), content_type='application/json')

    bg_updater_service.update_all_games(skip_recently_modified=False)
    return app.response_class(json.dumps('Sync Complete'), content_type='application/json')


@app.route('/syncNew', methods=['POST'])
def sync_new_games():
    print('Attempting to Update New Games')
    # Syncing Newly Added Games
    bg_updater_service = BgDbUpdaterService()
    locked = bg_updater_service.get_lock_status()

    if locked:
        return app.response_class(json.dumps('Database is Locked. Try again later'), content_type='application/json')

    bg_updater_service.update_new_games()
    return app.response_class(json.dumps('Sync Complete'), content_type='application/json')


@app.route('/testSync', methods=['GET'])
def test_sync_one_game():
    bg_updater_service = BgDbUpdaterService()
    locked = bg_updater_service.get_lock_status()

    if locked:
        return app.response_class(json.dumps('Database is Locked. Try again later'), content_type='application/json')
    else:
        bg_updater_service.test_update()
    return app.response_class(json.dumps('Test Sync Complete'), content_type='application/json')


@app.route('/lockStatus', methods=['GET'])
def check_lock_status():
    bg_updater_service = BgDbUpdaterService()
    is_locked = bg_updater_service.get_lock_status()
    return app.response_class(json.dumps(is_locked), content_type='application/json')


@app.route('/unlock', methods=['POST'])
def unlock_database():
    bg_updater_service = BgDbUpdaterService()
    bg_updater_service.unlock_database()
    return app.response_class(json.dumps("UNLOCKED"), content_type='application/json')


@app.route('/lock', methods=['POST'])
def lock_database():
    bg_updater_service = BgDbUpdaterService()
    bg_updater_service.lock_database()
    return app.response_class(json.dumps("LOCKED"), content_type='application/json')


# if __name__ == "__main__":
#     port = int(os.environ.get('PORT', 8085))
#     app.run(debug=app.config['DEBUG'], host='0.0.0.0', port=port)
#     # app.run()


if __name__ == "__main__":
    app.run(debug=app.config['DEBUG'], host="0.0.0.0", port=5000)
