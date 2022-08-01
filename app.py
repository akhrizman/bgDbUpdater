from flask import Flask, jsonify, request
from BgDbUpdaterService import BgDbUpdaterService
import os

app = Flask(__name__)   # Flask constructor


@app.route('/')
def hello():
    return 'BG Database Updater'


@app.route('/get', methods=['GET'])
def test_get():
    if request.method == 'GET':
        data = {
            "testGet": "Get Works",
        }
        return jsonify(data)


@app.route('/post', methods=['POST'])
def test_post():
    if request.method == 'POST':
        data = {
            "testPost": "Post Works",
        }
        return jsonify(data)


@app.route('/syncAll/', defaults={'update_recent': 'skip'}, methods=['GET'])
@app.route('/syncAll/<update_recent>', methods=['GET'])
def sync_all_games(update_recent):
    bg_updater_service = BgDbUpdaterService()
    locked = bg_updater_service.get_lock_status()

    if locked:
        return 'Database is Locked. Try again later'

    if update_recent == 'force':
        # Syncing All
        bg_updater_service.update_all_games(skip_recently_modified=False)
    else:
        # Syncing All Except Recently Synced
        bg_updater_service.update_all_games(skip_recently_modified=True)

    return 'Sync Complete'


@app.route('/testSync', methods=['GET'])
def test_sync_one_game():
    bg_updater_service = BgDbUpdaterService()
    locked = bg_updater_service.get_lock_status()

    if locked:
        return 'Database is Locked. Try again later'
    else:
        bg_updater_service.test_update()
    return 'Test Sync Complete'


@app.route('/syncNew', methods=['GET'])
def sync_new_games():
    print('Attempting to Update New Games')
    # Syncing Newly Added Games
    bg_updater_service = BgDbUpdaterService()
    locked = bg_updater_service.get_lock_status()

    if locked:
        return 'Database is Locked. Try again later'

    bg_updater_service.update_new_games()
    return 'Sync Complete'


@app.route('/lockStatus', methods=['GET'])
def check_lock_status():
    bg_updater_service = BgDbUpdaterService()
    locked = bg_updater_service.get_lock_status()
    return 'LOCKED' if locked else 'UNLOCKED'


@app.route('/unlock', methods=['POST'])
def unlock_database():
    bg_updater_service = BgDbUpdaterService()
    bg_updater_service.unlock_database()
    return 'UNLOCKED'


@app.route('/lock', methods=['POST'])
def lock_database():
    bg_updater_service = BgDbUpdaterService()
    bg_updater_service.lock_database()
    return 'LOCKED'


if __name__ == "__main__":
    port = int(os.environ.get('PORT', 8085))
    app.run(debug=True, host='0.0.0.0', port=port)
