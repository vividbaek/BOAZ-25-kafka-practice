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

<<<<<<< HEAD
# 1. DLQ로 메시지를 보낼 프로듀서를 미리 생성합니다.
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    # DLQ로는 원본 메시지(문자열)를 그대로 보내므로, utf-8 인코딩만 수행합니다.
    # ...(여기에 코드 추가 1줄) - 람다 함수 추가
    value_serializer=lambda v: v.encode('utf-8')
)

# 2. 카프카 컨슈머의 자동 커밋 기능을 끕니다.
=======

# 💡 미션 수행을 위해 컨슈머 내부에 프로듀서를 만들어야 합니다.
# 카프카는 바이트 배열을 전송하므로, utf-8 인코딩을 해야 합니다. (utf-8 인코딩을 해야 카프카가 바이트 배열을 전송할 수 있음)
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    #(...코드 1줄 추가...)
)

# 카프카 컨슈머 설정
>>>>>>> 931f9f138fe4194f6745f58e1ccbfe422443fbbf
consumer = KafkaConsumer(
    main_topic,
    bootstrap_servers='localhost:9092',
    group_id=consumer_group_id,
    auto_offset_reset='earliest',
<<<<<<< HEAD
    enable_auto_commit=False, # 수동 커밋을 위해 False로 변경함
=======
    enable_auto_commit=False, 
>>>>>>> 931f9f138fe4194f6745f58e1ccbfe422443fbbf
    value_deserializer=lambda m: m.decode('utf-8')
)

print(f"컨슈머 시작. '{main_topic}' 토픽의 메시지를 수신합니다.")

try:
    for message in consumer:
        print("-" * 50)
        print(f"수신: {message.value} (Partition: {message.partition}, Offset: {message.offset})")
        
<<<<<<< HEAD
        try:
            # 3. 메시지를 JSON으로 파싱 시도
            data = json.loads(message.value)
            print(f"✅ 처리 성공: {data}")
            
        except json.JSONDecodeError as e:
            # 4. 파싱 실패 시, DLQ 토픽으로 원본 메시지를 전송
            print(f"❌ 처리 실패 (JSON 파싱 불가): {e}")
            print(f"-> DLQ 토픽 '{dlq_topic}'으로 메시지를 전송합니다.")
            producer.send(dlq_topic, value=message.value) # 코드 수정후 주석 해제
        
        # 5. 성공하든 실패하든, 처리가 끝났으므로 오프셋을 수동으로 커밋
        # 이렇게 해야 다음 메시지로 넘어갈 수 있습니다.
        consumer.commit()

except KeyboardInterrupt:
    print("\n컨슈머 종료.")

finally:
    # 6. 종료 시 컨슈머와 프로듀서를 모두 안전하게 닫습니다.
    consumer.close()
    producer.close() # 코드 수정후 주석 해제
=======
        # 2. 성공 시 데이터 처리 로직 수행 (여기서는 출력으로 대체)
        print(f"✅ 처리 성공: {data}")

        # 💡 미션: 처리가 성공했으니, 오프셋을 수동으로 커밋해야 합니다.
        # consumer.commit()

    except json.JSONDecodeError as e:
        print(f"❌ 처리 실패 (JSON 파싱 불가): {e}")

        # 💡 미션: 파싱에 실패한 메시지를 DLQ 토픽으로 보내세요.
        # producer.send(dlq_topic, value=message.value) #producer 코드 수정후 주석해제
        
        # DLQ로 보낸 후에도 오프셋은 커밋해서 넘어가야 합니다.
        consumer.commit()

    except KeyboardInterrupt:
        print("\n컨슈머 종료.")

    finally:
        # 6. 종료 시 컨슈머와 프로듀서를 모두 안전하게 닫습니다.
        consumer.close()
        # producer.close() #producer 코드 수정후 주석해제
>>>>>>> 931f9f138fe4194f6745f58e1ccbfe422443fbbf
