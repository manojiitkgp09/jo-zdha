import os

import cherrypy
import jinja2

from myredis import MyRedis

from run import Crawler

root_path = os.path.dirname(__file__)

# jinja2 template renderer
env = jinja2.Environment(loader=jinja2.FileSystemLoader(os.path.join(root_path, 'templates')))


def render_template(template, **context):
    global env
    template = env.get_template(template+'.jinja')
    return template.render(context)


class Stocky(object):

    _cp_config = {
        'tools.encode.on': True,
        'tools.encode.encoding': 'utf8',
    }
    mr = MyRedis()

    def render(self, stocks, **kwargs):
        return render_template('index', stocks=stocks, columns=self.mr.columns, **kwargs)

    @cherrypy.expose()
    def index(self):
        top_ten_stocks = self.mr.get_top_ten()
        return self.render(top_ten_stocks)

    @cherrypy.expose()
    def search(self, name):
        stocks_by_name = self.mr.get_stock_by_name(name)
        return self.render(stocks_by_name, stock_name=name)

    @cherrypy.expose()
    def refresh(self):
        BASE_URL = "https://www.bseindia.com/"
        HOMEPAGE_PATH = "/markets/MarketInfo/BhavCopy.aspx"
        DOWNLOAD_LINK_ELEMENT_ID = "ContentPlaceHolder1_btnhylZip"
        FIELD_CSV_MAP = {
            'code': 'SC_CODE',
            'name': 'SC_NAME',
            'open': 'OPEN',
            'high': 'HIGH',
            'low': 'LOW',
            'close': 'CLOSE'
        }  # add if needed 'turnover': 'NET_TURNOV'
        r = MyRedis()
        crawler = Crawler(BASE_URL, DOWNLOAD_LINK_ELEMENT_ID, FIELD_CSV_MAP, r)
        home_page = crawler.get_download_page(HOMEPAGE_PATH)
        zip_dload_url = crawler.get_download_url(home_page)
        zipfile_obj = crawler.get_zip(zip_dload_url)
        extracted_csv_filename = crawler.extract_csv_from_zip(zipfile_obj)
        crawler.process_csv_to_db(extracted_csv_filename)
        return "Bazzzzzzzzzingaaaa"


if __name__ == '__main__':
    cherrypy.config.update({'server.socket_host': '0.0.0.0',})
    cherrypy.config.update({'server.socket_port': int(os.environ.get('PORT', '5000')),})
    conf = {
        '/': {
            'tools.staticdir.root': os.path.abspath(os.getcwd())
        },
        '/static': {
            'tools.staticdir.on': True,
            'tools.staticdir.dir': './public'
        }
    }
    cherrypy.quickstart(Stocky(), '/', conf)
