# dataproject2

DS2002 DataProject2
Project Members: Shawn Malik and Maggie Dykstra

Our Flask chatbot is live at `http://34.21.10.71:5001/chat`.  

To test:
In Terminal or Postman, run
```
curl -X POST http://34.21.10.71:5001/chat \
     -H "Content-Type: application/json" \
     -d '{"message":"Stephen Curry"}'
```

Our project uses an ETL Pipeline referning `player_data.csv` and the BallDontLie API.
