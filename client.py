#!/usr/bin/env python
#####################################################################################
# Urbanova Python Class
# Version: 1.01
# Python: 2.7.9
# Author: Nathan Scott
# Email: nathan.scott@wsu.edu
# Updated: 08/02/2017
# Copyright: Remote Sensing Laboratory | Washington State University
#####################################################################################

import hashlib
import uuid
import os
import time
import re
import serial
import urllib3
import random
import json
import pika
import uuid
from pprint import pprint
import mysql.connector
from mysql.connector import errorcode


class Client:
    #################################################################################
    # Function: rabbitMQclient()
    # Return: n/a.
    # Application: to connect rabbitMQ system.
    # Version: 1.00
    # Updated: 08/01/2017
    #################################################################################
    def rabbitMQclient(self, username, password, ip, port):
        # parameters = pika.URLParameters('amqp://user:re$earch@35.185.228.239:5672/%2F')
        parameters = pika.URLParameters("amqp://" + username + ":" + password + "@" + ip + ":" + port + "/%2F")
        self.connection = pika.BlockingConnection(parameters)
        self.channel = self.connection.channel()
        result = self.channel.queue_declare(exclusive=True)
        self.callback_queue = result.method.queue
        self.channel.basic_consume(self.on_response, no_ack=False,
                                   queue=self.callback_queue)

    #################################################################################
    # Function: on_response()
    # Return: n/a.
    # Application: RabbitMQ system.
    # Version: 1.00
    # Updated: 08/01/2017
    #################################################################################
    def on_response(self, ch, method, props, body):
            if self.corr_id == props.correlation_id:
                self.response = body

    #################################################################################
    # Function: call()
    # Return: a string.
    # Application: to receive a data package from a queue.
    # Version: 1.00
    # Updated: 08/01/2017
    #################################################################################
    def call(self, input, q):
        self.response = None
        self.corr_id = str(uuid.uuid4())
        self.channel.basic_publish(exchange = '',
                                   routing_key = q,
                                   properties=pika.BasicProperties(
                                         reply_to = self.callback_queue,
                                         correlation_id = self.corr_id,
                                         ),
                                   body=str(input))
        while self.response is None:
            self.connection.process_data_events()
        return str(self.response)

    #################################################################################
    # Function: conn()
    # Return: connection type object.
    # Application: to establish a connection between this device to a MYSQL database.
    # Version: 1.01
    # Updated: 07/24/2017
    #################################################################################
    def conn(self, username, password, ip, database):
        try:
            conn = mysql.connector.connect(user=username,
                                           passwd=password,
                                           host=ip,
                                           db=database)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Username or password is wrong!")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database or table does not exist!")
            else:
                print(err)
        else:
            return conn


    #################################################################################
    # Function: readMeta()
    # Return: a string.
    # Application: to read a configuration from a local file.
    # Version: 1.00
    # Updated: 07/22/2017
    #################################################################################
    def readMeta(self, filepath):
        with open(filepath) as f:    
            data = json.load(f)
            print("[ Meta ] " + repr(data) + "\n")
            # pprint(repr(data))

        f.close()
        return data


    #################################################################################
    # Function: genSig()
    # Return: a 128 bits hex string.
    # Application: to generate a string based on MD5 algorithm.
    # Version: 1.00
    # Updated: 07/22/2017
    #################################################################################
    def genSig(self,salt, meta):
        code = meta["sensor1"][0]["name"] + meta["sensor1"][0]["sn"] + meta["sensor1"][0]["calibration"] + \
               meta["sensor2"][0]["name"] + meta["sensor2"][0]["sn"] + meta["sensor2"][0]["calibration"] + \
               meta["sensor3"][0]["name"] + meta["sensor3"][0]["sn"] + meta["sensor3"][0]["calibration"] + \
               meta["sensor4"][0]["name"] + meta["sensor4"][0]["sn"] + meta["sensor4"][0]["calibration"] + \
               meta["sensor5"][0]["name"] + meta["sensor5"][0]["sn"] + meta["sensor5"][0]["calibration"] + \
               meta["sensor6"][0]["name"] + meta["sensor6"][0]["sn"] + meta["sensor6"][0]["calibration"]
        # print(repr(sn))
        # based on current data field design, md5/sha1 both ok.
        return str(hashlib.md5(salt.encode() + code.encode()).hexdigest())


    #################################################################################
    # Function: checkSig()
    # Return: True/False.
    # Application: to check if signature already exist in the database or not.
    # Version: 1.00
    # Updated: 07/22/2017
    #################################################################################
    def checkSig(self, conn, stationid, sig):
        try:
            sql = """SELECT sig FROM meta WHERE stationid = '%s' AND sig = '%s' ORDER BY mid DESC LIMIT 1""" % (stationid, sig)
            print("[ Query ] " + repr(sql) + "\n")
            
            cur = conn.cursor()
            cur.execute(sql)
            result = cur.fetchone()
            conn.commit()
            print("[ Check Signature ] " + repr(result) + "\n")
            
            if result == None:
                print("[ Signature ] " + sig + " does not exist in the database!\n")
                return False
            
            if result[0] == sig:
                print("[ Signature ] " + result[0] + " found in the database!\n")
                return True

        except mysql.connector.Error as err:
            conn.rollback()


    #################################################################################
    # Function: insertData()
    # Return: True/False.
    # Application: to insert a data record into a database.
    # Version: 1.00
    # Updated: 07/22/2017
    #################################################################################
    def insertData(self, conn, table, field, value):
        try:
            sql = """INSERT INTO %s (%s) VALUES(%s)""" % (table, field, value)
            print(repr(sql))
            cur = conn.cursor()
            cur.execute(sql)
            conn.commit()
            return True
        except mysql.connector.Error as err:
            print(err)
            conn.rollback()
            return False


    #################################################################################
    # Function: updateData()
    # Return: True/False.
    # Application: to update a data record into a database.
    # Version: 1.00
    # Updated: 07/24/2017
    #################################################################################
    def updateData(self, conn, mid):
        try:
            sql = """UPDATE meta SET dtime='%s' WHERE mid='%s'""" % (time.strftime("%Y-%m-%d") + " " + time.strftime("%H:%M:%S"), mid)
            print(repr(sql))
            cur = conn.cursor()
            cur.execute(sql)
            conn.commit()
        except mysql.connector.Error as err:
            print(err)
            conn.rollback()
            return False


    #################################################################################
    # Function: lastRecord()
    # Return: True/False.
    # Application: to find last record that accosiet with stationid in database.
    # Version: 1.00
    # Updated: 07/22/2017
    #################################################################################
    def lastRecord(self, conn, stationid):
        try:
            sql = """SELECT mid,stationid FROM meta WHERE stationid = '%s' ORDER BY mid DESC LIMIT 1""" % stationid
            print("[ Query ] " + repr(sql) + "\n")

            cur = conn.cursor()
            cur.execute(sql)
            result = cur.fetchone()
            conn.commit()
            # print(type(stationid))
            # print(stationid)
            # print(type(result[1]))
            # print(result[1])

            if result == None:
                print("[ Meta ID ] " + stationid + " does not exist in the database!")
                return False
            
            if result[1] == int(stationid):
                print("[ Meta ID ] " + str(result[0]) + " found in the database!")
                return result[0]
            
        except mysql.connector.Error as err:
            print(err)
            conn.rollback()
            return False


    #################################################################################
    # Function: genData()
    # Return: a string.
    # Application: to generate a data string for demo the program.
    # Version: 1.00
    # Updated: 08/02/2017
    #################################################################################
    def genData(self):
        data = "1000" + str(random.randint(1,9)) + \
                           "," + time.strftime("%Y%m%d") + \
                           "," + time.strftime("%H%M%S")
        return data

        
    #################################################################################
    # Function: run()
    # Return: n/a.
    # Application: to execute the program.
    # Version: 1.00
    # Updated: 08/02/2017
    #################################################################################
    def run(self, conf, data):
        try:
            while (True):
                conn = self.conn(conf["mysqlUsername"], conf["mysqlPassword"], conf["mysqlIP"], conf["mysqlDatabase"])
                print(conn)
                
                num = random.randint(1,5)
                    
                meta = self.readMeta(conf["metaFile"])
                
                sig = str(self.genSig(conf["signatureSalt"], meta))
                print("[ Signature ] " + sig + "\n")

                checkSig = self.checkSig(conn, meta["stationid"], sig)
                print(repr(checkSig) + "\n")
     
                self.rabbitMQclient(conf["rabbitMQusername"], conf["rabbitMQpassword"], conf["rabbitMQip"], conf["rabbitMQport"])

            
                if checkSig == False:    
                    lastRecord = self.lastRecord(conn, meta["stationid"])
                    print("[ Last Record ] " + repr(lastRecord) + "\n")

                    table = conf["mysqlMetaTable"]
                    field = conf["mysqlMetaField"]
                    value = "," + sig + "," + meta["stationid"] + "," + \
                            meta["sensor1"][0]["name"] + "," + meta["sensor1"][0]["sn"] + "," + meta["sensor1"][0]["calibration"] + "," + \
                            meta["sensor2"][0]["name"] + "," + meta["sensor2"][0]["sn"] + "," + meta["sensor2"][0]["calibration"] + "," + \
                            meta["sensor3"][0]["name"] + "," + meta["sensor3"][0]["sn"] + "," + meta["sensor3"][0]["calibration"] + "," + \
                            meta["sensor4"][0]["name"] + "," + meta["sensor4"][0]["sn"] + "," + meta["sensor4"][0]["calibration"] + "," + \
                            meta["sensor5"][0]["name"] + "," + meta["sensor5"][0]["sn"] + "," + meta["sensor5"][0]["calibration"] + "," + \
                            meta["sensor6"][0]["name"] + "," + meta["sensor6"][0]["sn"] + "," + meta["sensor6"][0]["calibration"] + "," + \
                            time.strftime("%Y-%m-%d") + " " + time.strftime("%H:%M:%S") + "," + ""

                    meta = "meta," + str(lastRecord) + str(value)
                    
                    # Queue Meta
                    start = time.time()
                    print (" [S] Sending Meta: %s" % meta)
                    response = self.call(str(meta), conf["rabbitMQqueue"])
                    print (" [R] Confirmation#: %r" % (response))
                    end = time.time()
                    print (" [T] %.6s (s)\n" % (end - start))
                    print("----------------------------------------------------------------\n")
                    # time.sleep(num)


                # print("MySQL database connection closed!\n")
                conn.close()

                # for demo only.
                data = ""
                if data == "":
                    data = self.genData()
                    # print(data)
                    
                # add tag in front of original data.
                if data[:4] != "data":
                    data = str(data)
                    data = "data," + str(data)
                
                # Queue Data
                start = time.time()
                print (" [S] Sending Data: %s" % data)
                response = self.call(data, conf["rabbitMQqueue"])
                print (" [R] Confirmation#: %r" % (response))
                end = time.time()
                print (" [T] %.6s (s)\n" % (end - start))
                print("----------------------------------------------------------------\n")
                # time.sleep(num)

                print("A MQ cycle has completed!\n")
                
                # remove this part before production
                time.sleep(random.randint(2, 5))
            
        except BaseException as err:
            print(err)

    
        
#####################################################################################
#####################################################################################

