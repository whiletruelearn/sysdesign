from datetime import timedelta
from socket import timeout


def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    
    import socket
    import selectors
    import types
    from datetime import datetime 
    s = socket.create_server(("localhost", 6379), reuse_port=True)
    sel = selectors.DefaultSelector()
    s.listen()
    s.setblocking(False)
    sel.register(s, selectors.EVENT_READ, data=None)

    data_store = {}
    ttl = {}
    
    def decode_message(echo_b):
        import string
        echo_b = str(echo_b)
        echo_b = echo_b[1:]
        tokens = echo_b.split("\\r\\n")
        out = []
        for tok in tokens:
            if tok.startswith(tuple(string.ascii_letters)) or tok.startswith(tuple(string.digits)):
                out.append(tok)
        return out


    def service_connection(key, mask):
        sock = key.fileobj
        data = key.data
        if mask & selectors.EVENT_READ:
            recv_data = sock.recv(1024)  # Should be ready to read
            if recv_data:
                print(f"received data {recv_data}")
                decode_out = decode_message(recv_data)
                print(f"decoded out {decode_out}")
                if decode_out[0] == "ping":

                    data.outb = b"+PONG\r\n"
                elif decode_out[0] == "echo":
                    data.outb = b"+" + decode_out[1].encode("ascii") + b"\r\n"

                elif decode_out[0].lower() == "set":
                    if len(decode_out) == 3:
                        k,v = decode_out[1], decode_out[2]
                        ttl[k] = datetime.now() + timedelta(days=365)
                    elif len(decode_out) == 5:
                        k,v, expiry =  decode_out[1], decode_out[2],decode_out[4]
                        ttl[k] = datetime.now() + timedelta(milliseconds=int(expiry))
                    
                    data_store[k] = v 
                    data.outb = b"+OK\r\n"

                elif decode_out[0].lower() == "get":
                    k = decode_out[1]
                    curr_time = datetime.now()
                    ttl_v = ttl.get(k,None)
                    if ttl_v is None:
                        data.outb = b"+(nil)\r\n"
                    elif ttl_v < curr_time:
                        del data_store[k]
                        del ttl[k]
                        data.outb = b"$-1\r\n"                 
                    else:

                        v = data_store.get(k,None) 
                        if v:
                            data.outb = b"+" + v.encode("ascii") +  b"\r\n"
                
                
            else:
                print(f"Closing connection to {data.addr}")
                sel.unregister(sock)
                sock.close()
        if mask & selectors.EVENT_WRITE:
            if data.outb:
                print(f"Echoing {data.outb!r} to {data.addr}")
                sent = sock.send(data.outb)  # Should be ready to write
                data.outb = data.outb[sent:]
                

    def accept_wrapper(sock):
        conn, addr = sock.accept()  
        print(f"Accepted connection from {addr}")
        conn.setblocking(False)
        data = types.SimpleNamespace(addr=addr, inb=b"", outb=b"")
        events = selectors.EVENT_READ | selectors.EVENT_WRITE
        sel.register(conn, events, data=data)

    while True:
        events = sel.select(timeout=None)
        for key,mask in events:
            if key.data is None:
                accept_wrapper(key.fileobj)
            else:
                service_connection(key,mask)
        
if __name__ == "__main__":
    main()
