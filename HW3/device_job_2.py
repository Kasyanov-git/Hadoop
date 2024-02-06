from pyflink.common import SimpleStringSchema
from pyflink.common.typeinfo import Types, RowTypeInfo
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream.connectors import DeliveryGuarantee
from pyflink.datastream.connectors.kafka import KafkaSource, \
    KafkaOffsetsInitializer, KafkaSink, KafkaRecordSerializationSchema
from pyflink.datastream.formats.json import JsonRowDeserializationSchema

from pyflink.datastream import StreamExecutionEnvironment, CheckpointingMode
from pyflink.datastream.checkpoint_storage import FileSystemCheckpointStorage

from pyflink.datastream.functions import MapFunction, ReduceFunction
from pyflink.datastream.window import TumblingEventTimeWindows, SlidingEventTimeWindows, EventTimeSessionWindows
from pyflink.datastream.window import Time


# Функция маппинга для конвертации значения температуры из Кельвинов в Цельсии
class TemperatureConversion(MapFunction):
    def map(self, value):
        return (value['device_id'], value['temperature'] - 273, value['execution_time'])

# Функция редьюсера для поиска максимальной температуры
class MaxTemperatureReducer(ReduceFunction):
    def reduce(self, value1, value2):
        return (value1[0], max(value1[1], value2[1]), value1[2])
def python_data_stream_example():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10000)  # включить checkpoint каждые 10 секунд
    env.get_checkpoint_config().set_checkpointing_mode(CheckpointingMode.EXACTLY_ONCE)

#Блок 1 Задание 1-------------------------------------------------------------
    checkpoint_storage = FileSystemCheckpointStorage("file:///opt/pyflink/tmp/checkpoints/logs")
    env.get_checkpoint_config().set_checkpoint_storage(checkpoint_storage)
# -------------------------------------------------------------

#Блок 1 Задание 2-------------------------------------------------------------
    # checkpoint_storage_hdfs = FileSystemCheckpointStorage("hdfs://hdfs:9870/flink/checkpoints")
    # env.get_checkpoint_config().set_checkpoint_storage(checkpoint_storage_hdfs)
# -------------------------------------------------------------

    type_info: RowTypeInfo = Types.ROW_NAMED(['device_id', 'temperature', 'execution_time'],
                                             [Types.LONG(), Types.DOUBLE(), Types.INT()])

    json_row_schema = JsonRowDeserializationSchema.builder().type_info(type_info).build()

    source = KafkaSource.builder() \
        .set_bootstrap_servers('localhost:9092') \
        .set_topics('itmo2023') \
        .set_group_id('pyflink-e2e-source') \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(json_row_schema) \
        .build()

    sink = KafkaSink.builder() \
        .set_bootstrap_servers('localhost:9092') \
        .set_record_serializer(KafkaRecordSerializationSchema.builder()
                               .set_topic('itmo2023')
                               .set_value_serialization_schema(SimpleStringSchema())
                               .build()
                               ) \
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE) \
        .build()

    ds = env.from_source(source, WatermarkStrategy.no_watermarks(), "Kafka Source")
    ds = ds.map(TemperatureConversion())
# Блок 2 Задание 1-------------------------------------------------------------
    # Применение Tumbling Windows
    ds.key_by(lambda value: value[1]) \
        .window(TumblingEventTimeWindows.of(Time.seconds(10))) \
        .reduce(MaxTemperatureReducer()) \
        .sink_to(sink)
# -------------------------------------------------------------

# Блок 2 Задание 2-------------------------------------------------------------
    # Применение Sliding Windows
    # ds.key_by(lambda value: value[1]) \
    #   .window(SlidingEventTimeWindows.of(Time.minutes(5), Time.minutes(1))) \
    #   .reduce(MaxTemperatureReducer()) \
    #   .sink_to(sink)
# -------------------------------------------------------------

# Блок 2 Задание 3-------------------------------------------------------------
    # Применение Session Windows
    # ds.key_by(lambda value: value[1]) \
    #   .window(EventTimeSessionWindows.with_gap(Time.minutes(10))) \
    #   .reduce(MaxTemperatureReducer()) \
    #   .sink_to(sink)
# -------------------------------------------------------------
    env.execute_async("Devices preprocessing")

class TemperatureFunction(MapFunction):

    def map(self, value):
        device_id, temperature, execution_time = value
        return {"device_id": device_id, "temperature": temperature - 273, "execution_time": execution_time}


if __name__ == '__main__':
    python_data_stream_example()
