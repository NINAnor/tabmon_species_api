# tabmon-species-api

backend:

merged_predictions_light: one folder per country, inside one folder per device, and then parquet files with the predictions

site_info: contains the site metadata, important because the deploymentID is in both merged_predictions_light and site_info, so can be used to join the site

proj_tabmon_NINA, proj_tabmon_NINA_ES ... contains the raw audio files, which will 

frontend:

streamlit where user can select country, site and species. after selection there should be the name of the species that have been detected at that site with birdnet confidence scores and timestamp. then the audio files should be playable in the streamlit app.