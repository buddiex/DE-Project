kafka-topics  --topic my-topic --describe --bootstrap-server kafka:29092
#list all topics
kafka-topics --list --bootstrap-server kafka:29092

#count messages in a topic per partition
kafka-run-class kafka.tools.GetOffsetShell  \
            --broker-list kafka:29092 \
            --topic home-sensehat-temperature   #awk -F  ":" '{sum += $3} END {print sum}'

kafka-run-class kafka.admin.ConsumerGroupCommand \
    --bootstrap-server localhost:29092 \
    --describe \
    --all-groups 

kafka-console-producer --topic quickstart-events --bootstrap-server localhost:29092
kafka-console-consumer --topic quickstart-events --from-beginning --bootstrap-server localhost:29092
kafka-console-consumer --topic home-sensehat-temperature --from-beginning --bootstrap-server localhost:29092

