# consumer.py

import json
from kafka import KafkaConsumer, KafkaProducer

# ======================================================================
# 🎯 미션:
# 이 코드는 불량 메시지를 만나면 에러 로그만 찍고 넘어갑니다.
# 1. 불량 메시지를 'event-stream-dlq' 토픽으로 보내세요.
# 2. 메시지 처리를 수동 커밋 방식으로 변경하여 신뢰성을 높이세요.
# ======================================================================

main_topic = 'event-stream'
dlq_topic = 'event-stream-dlq'
consumer_group_id = 'event-consumer-group'

# 카프카 컨슈머 설정
consumer = KafkaConsumer(
    main_topic,
    bootstrap_servers='localhost:9092',
    group_id=consumer_group_id,
    auto_offset_reset='earliest',
    # 😱 미션 수행을 위해 'enable_auto_commit'을 False로 바꿔야 합니다.
    enable_auto_commit=True, 
    value_deserializer=lambda m: m.decode('utf-8')
)

# 💡 미션 수행을 위해 컨슈머 내부에 프로듀서를 만들어야 합니다.
# producer = KafkaProducer(...)

print(f"컨슈머 시작. '{main_topic}' 토픽의 메시지를 수신합니다.")

for message in consumer:
    print("-" * 50)
    print(f"수신: {message.value} (Partition: {message.partition}, Offset: {message.offset})")
    
    try:
        # 1. 메시지를 JSON으로 파싱 시도
        data = json.loads(message.value)
        
        # 2. 성공 시 데이터 처리 로직 수행 (여기서는 출력으로 대체)
        print(f"✅ 처리 성공: {data}")

        # 💡 미션: 처리가 성공했으니, 오프셋을 수동으로 커밋해야 합니다.
        # consumer.commit()

    except json.JSONDecodeError as e:
        print(f"❌ 처리 실패 (JSON 파싱 불가): {e}")

        # 💡 미션: 파싱에 실패한 메시지를 DLQ 토픽으로 보내세요.
        # producer.send(dlq_topic, value=message.value.encode('utf-8'))
        
        # 💡 미션: DLQ로 보낸 후에도 오프셋은 커밋해서 넘어가야 합니다.
        # consumer.commit()