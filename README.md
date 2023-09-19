# Streaming Analytics:
## Use Case
### Input Kafka topic: real time orders
### Data format: json
`
{
    'account_id': '12345',
    'product': "Keyboard",
    'amount': 3,
    'price': 150.00
}
`
### Goals:
- analyze every 5 seconds
    - Product wise total value: amount * price
    - Compute product summary
- Publish the product summary result to product_summary topic

## Design
```mermaid
flowchart LR;
    kafka_input(Kafka Input)-->event(Json to Faust Stream);
    event-->com(Compute Order Value);
    com-->agg(Window: five seconds);
    agg-->ps(Compute Product Summary)
    ps-->output(Kafka Output);
```

## Implementation
1. Producer service: generate order json data randomly and publish to kafka input
2. Consumer service: 
* consume stream data
* compute order value
* compute product summary in each window 5 seconds
* publish to kafka output
3. Kafka cluster
4. Kafdrop tool for viewing messing in kafka

## Testing
- Assuming we are running docker service
- Open terminal and change to project folder
- Run this command to start services

`
docker-compose -f docker_compose.yml up
`
- That above command will start kafka cluster, kafkrop, producer, customer
- We can view product_summary using http://localhost:9000
- There are other useful commands to work with this project

`
docker-compose -f docker_compose.yml down
`

`
docker-compose -f docker_compose.yml build
`