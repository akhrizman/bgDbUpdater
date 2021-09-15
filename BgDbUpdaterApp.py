from flask import Flask
from BgDbUpdaterService import BgDbUpdaterService

app = Flask(__name__)   # Flask constructor


@app.route('/')
def hello():
    return 'BG Database Updater'


@app.route('/syncAll/', defaults={'update_recent': 'skip'})
@app.route('/syncAll/<update_recent>')
def sync_all_games(update_recent):
    bg_updater_service = BgDbUpdaterService()
    locked = bg_updater_service.get_lock_status()

    if locked:
        return 'Database is Locked. Try again later'

    if update_recent == 'force':
        # Syncing All
        bg_updater_service.update_all_games()
    else:
        # Syncing All Except Recently Synced
        bg_updater_service.update_all_games(skip_recently_modified=True)

    return "Sync Complete"


@app.route('/syncNew')
def sync_new_games():
    # Syncing Newly Added Games
    bg_updater_service = BgDbUpdaterService()
    locked = bg_updater_service.get_lock_status()

    if locked:
        return 'Database is Locked. Try again later'

    bg_updater_service.update_new_games()
    return "Sync Complete"


@app.route('/lock')
def check_lock_status():
    bg_updater_service = BgDbUpdaterService()
    bg_updater_service.get_lock_status()
    return str(bg_updater_service.get_lock_status())


if __name__ == '__main__':
    app.debug = True
    app.run()
    app.run(debug=True)
