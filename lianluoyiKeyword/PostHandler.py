#coding=utf-8
from http.server import HTTPServer,BaseHTTPRequestHandler

import cgi
import lianluoyiKeyword
import urllib

class  PostHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        form = cgi.FieldStorage(
            fp=self.rfile,
            headers=self.headers,
            environ={'REQUEST_METHOD':'POST',
                     'CONTENT_TYPE':self.headers['Content-Type'],
                     }
        )
        self.send_response(200)
        self.end_headers()
        # self.wfile.write('Client: %sn ' % str(self.client_address))
        # self.wfile.write('User-agent: %sn' % str(self.headers['user-agent']))
        # self.wfile.write('Path: %sn'%self.path)
        # self.wfile.write('Form data:n')
        for field in form.keys():
            field_item = form[field]
            name = field_item.name
            value  = field_item.value
            result = lianluoyiKeyword.dfa(value)#文件大小(字节)
            #print len(filevalue)
            print (result)
        self.wfile.write(str(result).encode())


def StartServer():
    from http.server import HTTPServer
    sever = HTTPServer(("127.0.0.1",5000),PostHandler)
    sever.serve_forever()




if __name__=='__main__':
    StartServer()