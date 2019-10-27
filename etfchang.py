import sys
from socket import *
from socket import error as socket_error
import json
import time

TEAMNAME = "TEAMNAME"
production = "production"
production_port = 25000
testmode = 0

exchange_hostname = "test-exch-" + TEAMNAME if testmode else production
port = production_port
serverstatus = 0
f=open("etf.log",'a')
BOND = []
CAR = []
CHE = []
BDU = []
ALI = []
TCT = []
BAT = []
BOOK_BOND = {}
BOOK_CAR = {}
BOOK_CHE = {}
BOOK_BDU = {}
BOOK_ALI = {}
BOOK_TCT = {}
BOOK_BAT = {}
orderid = 0
position={}

def TCPconnect():
    global serverstatus
    s = socket(AF_INET,SOCK_STREAM)
    print ("Start connecting to the serve...")
    s.connect((exchange_hostname, port))
    print ("Server Connection Established.")
    serverstatus = 1
    return s.makefile('rw', 1)

def readex(exchange):
    #print([exchange.readline()])
    try:
        js = json.loads(str(exchange.readline()))
    except BaseException:
        print("readex err")
        js = json.loads(str(exchange.readline()))
        print(js)
    return js

def writeex(exchange, obj):
    print("writeex !")
    json.dump(obj, exchange)
    exchange.write("\n")
    f.write(time.asctime( time.localtime(time.time()) ))
    json.dump(obj, f)
    f.write('\n')
   

#collect price history of securities we are interested in trading
def server_info(exchange):
    global serverstatus
    count = 0
    print ("Updating Server info...",serverstatus)
    while(count<1000):
        try:
            info = readex(exchange)
        except BaseException:
            break
        if not info:
            break
        type = info["type"]
        if(type == "close"):
            serverstatus = 0;
            print ("Server closed.")
            return
        if(type == "book"):
            if(info["symbol"] == "BOND"):
                 BOOK_BOND={"buy":info["buy"],"sell":info["sell"]}
            elif(info["symbol"] == "CAR"):
                 BOOK_CAR={"buy":info["buy"],"sell":info["sell"]}
            elif(info["symbol"] == "CHE"):
                 BOOK_CHE={"buy":info["buy"],"sell":info["sell"]}
            elif(info["symbol"] == "ALI"):
                 BOOK_ALI={"buy":info["buy"],"sell":info["sell"]}
            elif(info["symbol"] == "TCT"):
                 BOOK_TCT={"buy":info["buy"],"sell":info["sell"]}
            elif(info["symbol"] == "BAT"):
                 BOOK_BAT={"buy":info["buy"],"sell":info["sell"]}
        elif(type == "ack" or type == "reject"  ):
            print(time.asctime( time.localtime(time.time()) ),info)
            f.write(time.asctime( time.localtime(time.time()) ))
            json.dump(info, f)
            f.write('\n')
        elif( type == "fill" ):
                print(time.asctime( time.localtime(time.time()) ),info)
                f.write(time.asctime( time.localtime(time.time()) ))
                json.dump(info, f)
                f.write('\n')
                position[info["symbol"]]+=info["size"]
                print(position)
        
        elif(type == "trade"):       
            if(info["symbol"] == "BOND"):
                BOND.append(info["price"])
                
            if(info["symbol"] == "CAR"):
                CAR.append(info["price"])

            if (info["symbol"] == "CHE"):
                CHE.append(info["price"])

            if (info["symbol"] == "BDU"):
                BDU.append(info["price"])

            if (info["symbol"] == "ALI"):
                ALI.append(info["price"])

            if (info["symbol"] == "TCT"):
                TCT.append(info["price"])

            if (info["symbol"] == "BAT"):
                BAT.append(info["price"])

        count += 1

def mean(l):
    return sum(l)//len(l)


def ADR(cs_trade_price_list, adr_trade_price_list):
    cs_mean = cs_trade_price_list[-1]
    adr_mean = adr_trade_price_list[-1]
    fair_diff = cs_mean - adr_mean
    if (fair_diff > 2):
        return [True, cs_mean, adr_mean]

def ADRR(cs_trade_price_list, adr_trade_price_list):
    cs_mean = cs_trade_price_list[-1]
    adr_mean = adr_trade_price_list[-1]
    fair_diff = adr_mean - cs_mean
    if (fair_diff > 2):
        return [True, cs_mean, adr_mean]

def etfArbitrageSignal(BAT_trade_price, bond_trade_price, BDU_trade_price, ALI_trade_price, TCT_trade_price):

    BAT_mean = mean(BAT_trade_price)
    bond_mean = mean(bond_trade_price)
    BDU_mean = mean(BDU_trade_price)
    ALI_mean = mean(ALI_trade_price)
    TCT_mean = mean(TCT_trade_price)

# 10 BAT
# 3 BOND
# 2 BDU
# 3 ALI
# 2 TCT
    #find long etf arbitrage opportunities
    if 10 * BAT_mean + 150 < (3 * bond_mean + 2 * BDU_mean + 3 * ALI_mean + 2 * TCT_mean):
        return ["long", BAT_mean, bond_mean, BDU_mean, ALI_mean, TCT_mean]
    #find short etf arbitrage opportunites
    if 10 * BAT_mean - 150 > (3 * bond_mean + 2 * BDU_mean + 3 * ALI_mean + 2 * TCT_mean):
        return ["short", BAT_mean, bond_mean, BDU_mean, ALI_mean, TCT_mean]

def action(exchange,bond):
    # while(True):
    global serverstatus
    global orderid
    writeex(exchange,{"type": "add", "order_id": orderid, "symbol": "BOND", "dir": "BUY", "price": 999, "size": 100})
    orderid += 1
    writeex(exchange,{"type": "add", "order_id": orderid, "symbol": "BOND", "dir": "SELL", "price": 1001, "size": 100})
    orderid += 1
    print("action start")

def actionADR(exchange,car,che):
    global serverstatus
    global orderid
    if(len(car) >= 10 and len(che) >= 10):
        car = car[-10:]
        che = che[-10:]
        result = ADR(car, che)
        result2 = ADRR(car,che)
        if(result != None and result[0] == True):
            print ("\n------------------------- ADR Make Action!-------------------------\n")
            orderid +=1
            writeex(exchange, {"type" : "add", "order_id": orderid, "symbol": "CHE", "dir" : "BUY",
                                         "price": result[2], "size": 10 - position['CHE']})
            # orderid += 1
            # writeex(exchange, {"type" : "convert", "order_id": orderid, "symbol": "CHE", "dir" : "SELL",
            #                              "size": 10 - position['CHE']})
            
            orderid +=1
            writeex(exchange, {"type" : "add", "order_id": orderid, "symbol": "CAR", "dir" : "SELL",
                                         "price": result[1], "size": 10 + position['CAR']})
        if(result2 != None and result2[0] == True):
            print ("\n------------------------- ADR2 Make Action!-------------------------\n")
            orderid +=1
            writeex(exchange, {"type" : "add", "order_id": orderid, "symbol": "CAR", "dir" : "BUY",
                                         "price": result2[1], "size": 10 - position['CAR']})
            # orderid += 1
            # writeex(exchange, {"type" : "convert", "order_id": orderid, "symbol": "CHE", "dir" : "BUY",
            #                              "size": 10})
            
            orderid +=1
            writeex(exchange, {"type" : "add", "order_id": orderid, "symbol": "CHE", "dir" : "SELL",
                                         "price": result2[2], "size": 10 + position['CHE']})


def cancel(exchange,id):
    for i in range(id):
        writeex(exchange,{"type": "cancel", "order_id": i})

def actionETF(exchange, bat, bond, bdu, ali, tct):
    global orderid
    #ETF arbitrage trading
    if (len(bat) >25 and len(bond) >= 25 and len(bdu) >= 25 and len(ali) >= 25 and len(tct) >= 25):
        bat = bat[-25:]
        bond = bond[-25:]
        bdu = bdu[-25:]
        ali = ali[-25:]
        tct = tct[-25:]
        etf = etfArbitrageSignal(bat, bond, bdu, ali, tct)
        if (etf != None and etf[0] == 'long'):
            print("\n------------------------- ETF Long Make Action!-------------------------\n")
            orderid += 1
            writeex(exchange, {"type": "add", "order_id": orderid, "symbol": "BAT", "dir": "BUY",
                                         "price": etf[1], "size": 90})

            orderid += 1
            writeex(exchange, {"type": "convert", "order_id": orderid, "symbol": "BAT", "dir": "SELL", "size": 90})

            orderid += 1
            writeex(exchange, {"type": "add", "order_id": orderid, "symbol": "BOND", "dir": "SELL",
                                         "price": 1001, "size": 27})

            orderid += 1
            writeex(exchange, {"type": "add", "order_id": orderid, "symbol": "BDU", "dir": "SELL",
                                         "price": etf[3], "size": 18})

            orderid += 1
            writeex(exchange, {"type": "add", "order_id": orderid, "symbol": "ALI", "dir": "SELL",
                                         "price": etf[4], "size": 27})

            orderid += 1
            writeex(exchange, {"type": "add", "order_id": orderid, "symbol": "TCT", "dir": "SELL",
                                         "price": etf[5], "size": 18})

        if (etf != None and etf[0] == 'short'):
            print("\n------------------------- ETF SHORT Make Action!-------------------------\n")
            orderid += 1
            writeex(exchange, {"type": "add", "order_id": orderid, "symbol": "BOND", "dir": "BUY",
                "price": etf[2], "size": 27})

            orderid += 1
            writeex(exchange, {"type": "add", "order_id": orderid, "symbol": "BDU", "dir": "BUY",
                                         "price": etf[3], "size": 18})

            orderid += 1
            writeex(exchange, {"type": "add", "order_id": orderid, "symbol": "ALI", "dir": "BUY",
                                          "price": etf[4], "size": 17})

            orderid += 1
            writeex(exchange, {"type": "add", "order_id": orderid, "symbol": "TCT", "dir": "BUY",
                                         "price": etf[5], "size": 18})

            orderid += 1
            writeex(exchange, {"type": "convert", "order_id": orderid, "symbol": "BAT", "dir": "BUY", "size": 90})

            orderid += 1
            writeex(exchange, {"type": "add", "order_id": orderid, "symbol": "BAT", "dir": "SELL",
                                          "price": etf[1], "size": 90})

def reconnect(exchange):
    global serverstatus
    print ("\nMarket Closed. Reconnecting...\n")
    while(serverstatus == 0):
        try:
            print ("Reconnect: restablishing TCP connect")
            exchange = TCPconnect()
            writeex(exchange, {"type": "hello", "team": TEAMNAME.upper()})
            hello_from_exchange = readex(exchange)
            print ("Reconnec: message received: " "%s" % hello_from_exchange)
            if(hello_from_exchange["type"] == "hello"):
                serverstatus = 1
                print ("----------------Handshake Success!----------------")
            else:
                serverstatus = 0
                print ("----------------Handshake Error!----------------")
        except socket_error:
             print ("\r\nReconnect: socket error,do reconnect ")

def main():
    global serverstatus
    global orderid
    exchange = TCPconnect()
    print("Exchange Initialize Success.")

    writeex(exchange, {"type": "hello", "team": TEAMNAME.upper()})
    hello_from_exchange = readex(exchange)
    print("The exchange replied:", hello_from_exchange, file=sys.stderr)
    global position
    for i in hello_from_exchange['symbols']:
        position[i['symbol']]=i['position']
    while(True):
        server_info(exchange)
        print("server info")
        if(serverstatus == 1):
            #action(exchange,BOND)
            actionADR(exchange,CAR,CHE)
            actionETF(exchange,BAT,BOND,BDU,ALI,TCT)
            #print(BOND)
            cancel(exchange,orderid)

        else:
            exit(1)
            #reconnect(exchange)
        print("end")


def initialize():
    print ("Initializing Test Mode: ")
    print ("   Test Mode: " "%s" % testmode)
    print ("   Port: " "%s" % port)
    print ("   Hostname: " "%s" % exchange_hostname)

if __name__ == '__main__':
    initialize()
    while True:
         try:
             main()
         except socket_error:
             print ("\r\n----------------Main: socket error,do reconnect----------------")
