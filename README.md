# Alternator

Alternator is an alternative dynamodb client for scala, influenced by 
[Scanamo](scanamo.org).


## AttributeValue mapping
An almost perfectly scanamo compatible implementation of attribute-value mapping.

### Compatibility with Scanamo
 - case objects are only supported in sealed traits 
 - Scanamo does not support binary sets ([withBS](https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/dynamodbv2/model/AttributeValue.html#withBS-java.nio.ByteBuffer...-)),
    we map `Set[SdkBytes]`, `Set[ByteBuffer]` and `Set[Array[Byte]]` to binary set.
 - 

### Benchmark

ops/s|Alternator|Scanamo 1.0.0-M12-1|Scanamo 1.0.0-M15
---|---|---|---
read_1|1918044.318|454411.545|515801.483
read_10|204541.120|50401.314|42981.554
read_100|20651.595|4504.985|3659.635
read_1000|1655.991|154.344|135.671
read_10000|171.442|1.662|1.882
write_1|938950.438|665111.468|746413.784
write_10|81271.801|103485.906|83890.453
write_100|7998.594|12228.823|8889.472
write_1000|770.602|1191.022|864.936
write_10000|73.338|113.524|83.749
