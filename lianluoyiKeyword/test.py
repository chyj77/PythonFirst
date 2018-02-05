#coding=utf-8
import requests
url = "http://127.0.0.1:5000"
message = '四处乱咬乱吠，吓得家中11岁的女儿躲在屋里不敢出来，直到辖区派出所民警赶到后，才将孩子从屋中救出。最后在征得主人同意后，民警和村民合力将这只发疯的狗打死'

param = {'keyword': message}
r = requests.post(url, data=param)
print (r.url)
print (r.text)