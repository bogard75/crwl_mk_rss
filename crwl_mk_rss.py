import pandas as pd
import pymysql
import requests
from sqlalchemy import create_engine
from bs4 import BeautifulSoup


def crwl_mk_rss(ctgry, url):
    print("매경 {0} 카테고리를 긁습니다...".format(ctgry))
    #url = 'http://file.mk.co.kr/news/rss/rss_30100041.xml'  # 매경경제
    
    r = requests.get(url)
    soup = BeautifulSoup(r.content, 'html.parser')
    datas = soup.find_all('title')
    links = soup.find_all('link')
    
    def crwl_mk_article(link):
        try:
            r = requests.get(link)
            s = BeautifulSoup(r.content, 'html.parser')
            s.find_all('div', {'id':'article_body'})[0].find_all('div', {'class':'art_txt'})
            artcl = s.find_all('div', {'class':'art_txt'})[0]
        except Exception as e:
            print('[crwl_mk_article] error : ', e)    
            return 'error crwling article...'
        else:
            for i in artcl.find_all('div'): i.decompose()
            for i in artcl.find_all('script'): i.decompose()
            return artcl.text
    
    def df_to_aws(df, tbname):
        pymysql.install_as_MySQLdb()
        eng = create_engine("mysql+mysqldb://bogard75:__mypassword__@getstockpy.cwlv0262o99p.us-east-2.rds.amazonaws.com/getstockpy", encoding='utf-8')
        conn = eng.connect()
    
        try:
            df.to_sql(name=tbname, con=conn, if_exists='append')
        except Exception as e:
            print('[error]', e)
            df_to_aws(df, tbname)
        finally:
            conn.close()
        
    for n, i in enumerate(zip(datas, links)):
        # 기사 출력
        artcl_subject = i[0].text
        artcl_link = i[1].text
                           
        print('({0}) {1} [{2}]'.format(n, artcl_subject, artcl_link))
        
        # 기사 링크 스크래핑
        if n > 0:
            artcl_text = crwl_mk_article(artcl_link)
            df = pd.DataFrame({'category':[ctgry],
                               'subject':[artcl_subject],
                               'link':[artcl_link],
                               'article':[artcl_text]})
    #       r2 = requests.get(i[1].text)
    #       s2 = BeautifulSoup(r2.content, 'html.parser')
    #       s2.find_all('div', {'id':'article_body'})[0].find_all('div', {'class':'art_txt'})
    #       artcl = s2.find_all('div', {'class':'art_txt'})[0]
    #       for i in artcl.find_all('div'): i.decompose()
    #       for i in artcl.find_all('script'): i.decompose()
            print('aws writing....')
            df_to_aws(df, 'crwl_mk_article')

    print("rss 추출이 완료되었습니다.")


mk_rss = {'헤드라인':'http://file.mk.co.kr/news/rss/rss_30000001.xml',
          '전체뉴스':'http://file.mk.co.kr/news/rss/rss_40300001.xml',
          '경제':'http://file.mk.co.kr/news/rss/rss_30100041.xml',
          '정치':'http://file.mk.co.kr/news/rss/rss_30200030.xml',
          '사회':'http://file.mk.co.kr/news/rss/rss_50400012.xml',
          '국제':'http://file.mk.co.kr/news/rss/rss_30300018.xml',
          '기업ㆍ경영':'http://file.mk.co.kr/news/rss/rss_50100032.xml',
          '증권':'http://file.mk.co.kr/news/rss/rss_50200011.xml',
          '부동산':'http://file.mk.co.kr/news/rss/rss_50300009.xml',
          '문화ㆍ연예':'http://file.mk.co.kr/news/rss/rss_30000023.xml',
          '패션':'http://file.mk.co.kr/news/rss/rss_72000001.xml',
          '스포츠':'http://file.mk.co.kr/news/rss/rss_71000001.xml',
          '게임':'http://file.mk.co.kr/news/rss/rss_50700001.xml',
          '오피니언':'http://file.mk.co.kr/news/rss/rss_30500041.xml',
          '특집-MBA':'http://file.mk.co.kr/news/rss/rss_40200124.xml',
          '머니 앤 리치스':'http://file.mk.co.kr/news/rss/rss_40200003.xml',
          '영문-Top Stories':'http://file.mk.co.kr/news/rss/rss_30800011.xml',
          '이코노미':'http://file.mk.co.kr/news/rss/rss_50000001.xml',
          '시티라이프':'http://file.mk.co.kr/news/rss/rss_60000007.xml'}

for key in list(mk_rss.keys()):
    crwl_mk_rss(key, mk_rss[key])
