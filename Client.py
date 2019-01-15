import socket
import subprocess
import sys
import random

print("CS421 testing program, FALL 2018 BILKENT UNIVERSITY")
LEN_ARR = 100

port1 = "10000"
port2 = "10001"
port3 = "10002"
port4 = "10003"
port5 = "10004"

if(len(sys.argv) == 7):
    port1 = sys.argv[2]
    port2 = sys.argv[3]
    port3 = sys.argv[4]
    port4 = sys.argv[5]
    port5 = sys.argv[6]

ports = [port1, port2, port3, port4, port5]

occupancies = [round(random.random() * 100) for i in range(5)]
proportions = [1 / (u + 1) for u in occupancies]
denom = sum(proportions)
proportions = list(map(lambda x: x/denom, proportions))
indices = [int(LEN_ARR * p) for p in proportions]

processes = []
for i in range(5):
    processes.append(subprocess.Popen([sys.executable, 'Operator.py', ports[i], str(occupancies[i]), str(indices[i])], stdout=subprocess.PIPE, stderr=subprocess.STDOUT))

addr = sys.argv[1]
addr,port = addr.split(':')
port = int(port)

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect((addr, port))

# Numbers
numbers_list = random.choices(range(0, 30), k=LEN_ARR)
numbers = ",".join(map(str, numbers_list))
message_numbers = ("DATA:%s\n" % numbers).encode("utf-8")

# Functions
message_funcs = ("FUNCS:%s\n" % "f2,f1,f3,f1").encode("utf-8")

# Decide whether to send numbers or functions first
if random.random() > 0.5:
    s.sendall(message_numbers)
    s.sendall(message_funcs)
else:
    s.sendall(message_funcs)
    s.sendall(message_numbers)

# =============================================================================
#                               Error checking
# =============================================================================
try:
    print("Waiting for operators to terminate...")
    p_outs = []
    for p in processes:
        p_outs.extend(p.communicate(timeout=30)[0].decode("utf-8").splitlines())

except subprocess.TimeoutExpired:
    print("""Operators could not finish their jobs in 30 seconds. Some possible reasons:
1) Your code is extremely inefficient,
2) You forgot to send "END" message to the operators,
3) Operator processes from your previous experiments might be alive and interfering with the sockets. Try different ports.
""")
    for p in processes:
        p.kill()
    s.close()

else:      
    error_flag = False
    if "1" in p_outs:
        print("Too many elements sent to one of the operators. Please check your code.")
        error_flag = True
        
        while True:
            try:
                p_outs.remove("1")
            except:
                break
            
    if "2" in p_outs:
        print("Too few elements sent to one of the operators. Please check your code.")
        error_flag = True
        
        while True:
            try:
                p_outs.remove("2")
            except:
                break
           
    if len(p_outs) != 0:
        error_flag = True
        for e in p_outs:
            print(e)
         
    if error_flag:
        s.close()
# =============================================================================
#                               Error checking
# =============================================================================
        
        
    else:
        f = s.makefile(buffering=1, encoding="utf-8")
        l = f.readline()
        s.close()
    
        data = l[:-1].split(":")[1]        
        expected_numbers = [(number ** 2 * 17 + 53) * 17 for number in numbers_list]
        expected_numbers = ",".join(map(str, expected_numbers))    
        
        if data == expected_numbers:
            print("RESULTS ARE CORRECT, YAAAY!")
        
        else:        
            print("Received data:", data, "\n")
            print("Expected data:", expected_numbers, "\n")
            print("WRONG RESULTS, PLEASE CHECK YOUR CODE :(")
