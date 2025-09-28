# producer.py

import json
import time
from kafka import KafkaProducer

# 카프카 프로듀서 설정
# 팁: value_serializer는 딕셔너리는 json으로, 문자열은 그대로 utf-8로 인코딩합니다.
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8') if isinstance(v, dict) else v.encode('utf-8')
)

# 실습에서 사용한 토픽과 다른 이름을 사용해 혼동을 방지합니다.
topic_name = 'event-stream'
message_count = 0

print(f"'{topic_name}' 토픽으로 정상 메시지와 불량 메시지를 섞어서 전송합니다.")

try:
    while True:
        # 4의 배수 번째 메시지는 일부러 깨진 형식으로 전송
        if message_count % 4 == 0:
            message = '{"event_id": ' + str(message_count) + ', "data": "malformed json data"' # <-- JSON 형식을 일부러 망가뜨림
            print(f"-> 😠 불량 메시지 전송: {message}")
            producer.send(topic_name, value=message)
        else:
            # 정상 메시지 전송
            message = {'event_id': message_count, 'data': f'valid_data_{message_count}'}
            print(f"-> 😊 정상 메시지 전송: {message}")
            producer.send(topic_name, value=message)
        
        producer.flush()
        message_count += 1
        time.sleep(2)

except KeyboardInterrupt:
    print("\n프로듀서 종료.")
finally:
    producer.close()