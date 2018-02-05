from flask import Flask
import mystock
import lianluoyiKeyword

app = Flask(__name__)


# @app.route('/')
def hello_world():
    page = mystock.get_pages('http://q.10jqka.com.cn/index/index/board/all/field/zdf/order/desc/page/1/ajax/1/')
    for i in range(1,int(page)):
        url = 'http://q.10jqka.com.cn/index/index/board/all/field/zdf/order/desc/page/%s/ajax/1/' %(i)
        htmlText = mystock.get_content(url)
        print(mystock.get_data(htmlText))
    return

#@app.route('/<message>')
def myKeyword(message):
   result = lianluoyiKeyword.dfa(message)
   print(result)
   return str(result)

if __name__ == '__main__':
    hello_world()
    # keyword(message)
    app.run()
