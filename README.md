# dataproject2

## DS2002 DataProject2
## Project Members: Shawn Malik and Maggie Dykstra

Our Flask chatbot is live at `http://34.21.10.71:5001/chat`.  

Our project uses an ETL Pipeline referencing `player_data.csv` and the BallDontLie API. This is run thorough our pipeline to create the processed data in the folder which is used by our Discord Chatbot.

To use our Discord Bot:

Go to http://34.21.10.71:5001/chat and click the invite link to invite the Discord Bot to your server.

Example Command !player Stephen Curry

To test:
In Terminal or Postman, run
```
curl -X POST http://34.21.10.71:5001/chat \
     -H "Content-Type: application/json" \
     -d '{"message":"Stephen Curry"}'
```

