#!/usr/bin/env ruby -wW1

require 'socket'
s = TCPSocket.open("127.0.0.1", "8888")
s.write("*3\r\n$7\r\nPUBLISH\r\n$6\r\nquotes\r\n$75\r\nWhen Chuck Norris wants a steak, cows volunteer. It's just easier that way.\r\n")
puts s.gets.inspect

s.write("SUBSCRIBE quotes scores\r\n")
puts s.readpartial(1024).inspect

s.write("SUBSCRIBE movies episodes\r\n")
puts s.readpartial(1024).inspect

s.write("SHUTDOWN\r\n")
puts s.gets.inspect
