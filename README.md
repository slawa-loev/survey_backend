# GBQ database with SurveyJS frontend

## database interaction

To interact with the GBQ database use the `.env` file (provided seperately by the repository owner to you), in which you can set paths (to database credentials, also provided by the repo owner, and data folder) and formulate SQL-queries.

After you configured the `.env` file to your needs, simply execute `make query` in your terminal.

## API activation

In order to enable automatic database logging of results in the SurveyJS frontend (found [here](https://github.com/slawa-loev/survey_frontend)), activate the API by executing `make start_api` in your terminal. You can try it by invoking and filling out at the same time the survey (given you have the mentioned [SurveyJS frontend](https://github.com/slawa-loev/survey_frontend) repository) by navigating into the folder and execute `make survey`. It will appear in your browser and the results will be saved in the GBQ database via the API.
