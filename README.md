# Board Game Database Updater

## History:
- As a boardgamer and collector, I used to keep all of my games in an excel spreadsheet.
- Eventually, I created a MySQL database to store that information.
- I created a python script to access BoardGameGeek's API https://boardgamegeek.com/wiki/page/BGG_XML_API2 and keep my db up to date with data such as descriptions and user ratings 
- Later it evolved into a Flask App that can be triggered remotely to update games in my library with data from the BGG api, typically used when I add a new game to my library.
- I manage my library with a SpringBoot app which has a front-facing component to allow people to search my library

## Deployment
```bash
# Build the project
docker image build -t updater .

# Deploy dev - MUST BE in root directory of project
docker stop {current dev container}
docker run -dp 9085:5000 --name updater-dev -e FLASK_ENV=development updater

# Deploy both
docker stop {current prod container}
docker run -dp 8085:5000 --name updater-prod -e FLASK_ENV=production updater
```