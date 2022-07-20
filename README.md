# bgDbUpdater

History:
- As a boardgamer and collector, I used to keep all of my games in an excel spreadsheet.
- Eventually, I created a MySQL database to store that information.
- I created a python script to access BoardGameGeek's API https://boardgamegeek.com/wiki/page/BGG_XML_API2 and keep my db up to date with data such as descriptions and user ratings 
- Later it evolved into a Flask App that can be triggered remotely to update games in my library with data from the BGG api, typically used when I add a new game to my library.
- I manage my library with a SpringBoot app which has a front-facing component to allow people to search my library
