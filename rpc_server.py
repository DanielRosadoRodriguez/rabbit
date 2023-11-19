import pika


connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost', port=5672))

channel = connection.channel()

channel.queue_declare(queue='rpc_queue')


def get_file(n) -> str:
    if file_exists(n):
        file_name = search_file_on_index(n)
        return get_file_content(file_name)
    else:
        return "El archivo no existe"


def file_exists(index) -> bool:
    f = open("indices.txt", "r")
    with f:
        for line in f:
            if line.startswith(f"{index}"):
                return True
    return False


def search_file_on_index(n) -> str:
    f = open("indices.txt", "r")
    with f:
        for line in f:
            if line.startswith(f"{n}"):
                file_title = line.split(",")[1]
                return file_title.rstrip()
    return "no_se_encontro.txt"
    

def get_file_content(file_name:str) -> str:
    path: str = f"archivos/{file_name}"
    f = open(path, "r")
    with f:
        return f.read()

    
def on_request(ch, method, props, body):
    file_index:int = int(body)

    print(f" [.] Se accede al archivo: ({file_index})")
    file_content: str = get_file(file_index)

    response = file_content.replace("b'", "").replace("'", "")



    ch.basic_publish(exchange='',
                     routing_key=props.reply_to,
                     properties=pika.BasicProperties(correlation_id = \
                                                         props.correlation_id),
                     body=str(response))
    ch.basic_ack(delivery_tag=method.delivery_tag)

channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue='rpc_queue', on_message_callback=on_request)

print(" [x] Esperando solicitudes RPC")
channel.start_consuming()