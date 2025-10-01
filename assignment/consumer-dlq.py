# consumer.py (Answer Key)

import json
from kafka import KafkaConsumer, KafkaProducer

# ======================================================================
# 미션
# 1. 불량 메시지를 'event-stream-dlq' 토픽으로 보냅니다.
# 2. 메시지 처리를 수동 커밋 방식으로 변경하여 신뢰성을 높였습니다.
# ======================================================================

main_topic = 'event-stream'
dlq_topic = 'event-stream-dlq'
consumer_group_id = 'event-consumer-group'


# 💡 미션 수행을 위해 컨슈머 내부에 프로듀서를 만들어야 합니다.
# 카프카는 바이트 배열을 전송하므로, utf-8 인코딩을 해야 합니다. (utf-8 인코딩을 해야 카프카가 바이트 배열을 전송할 수 있음)
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    # 코드 1줄 추가)
)

consumer = KafkaConsumer(
    main_topic,
    bootstrap_servers='localhost:9092',
    group_id=consumer_group_id,
    auto_offset_reset='earliest',
    enable_auto_commit=False, 
    value_deserializer=lambda m: m.decode('utf-8')
)

print(f"컨슈머 시작. '{main_topic}' 토픽의 메시지를 수신합니다.")

try:
    for message in consumer:
        print("-" * 50)
        print(f"수신: {message.value} (Partition: {message.partition}, Offset: {message.offset})")
        
        try:
            # 3. 메시지를 JSON으로 파싱 시도
            data = json.loads(message.value)
            print(f"✅ 처리 성공: {data}")
            
        except json.JSONDecodeError as e:
            # 4. 파싱 실패 시, DLQ 토픽으로 원본 메시지를 전송
            print(f"❌ 처리 실패 (JSON 파싱 불가): {e}")
            print(f"-> DLQ 토픽 '{dlq_topic}'으로 메시지를 전송합니다.")
            # producer.send(dlq_topic, value=message.value) # 코드 수정 후 주석해제
        
        # 5. 성공하든 실패하든, 처리가 끝났으므로 오프셋을 수동으로 커밋
        # 이렇게 해야 다음 메시지로 넘어갈 수 있습니다.
        consumer.commit()

except KeyboardInterrupt:
    print("\n컨슈머 종료.")

finally:
    # 6. 종료 시 컨슈머와 프로듀서를 모두 안전하게 닫습니다.
    consumer.close()
    producer.close()