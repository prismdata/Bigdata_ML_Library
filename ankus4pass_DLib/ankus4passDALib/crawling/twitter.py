# -*- coding: utf-8 -*-
import tweepy
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

consumer_key='yXPhSCGufdwT0RfI2uwKNrg5w'
consumer_secret='nfJ4VK8IOT691PojlQ6qS5XoeOqXuaiirzj68iUYYjenBCpNDA'
access_token_key='208354086-hLd2w2tcnl4BGrSM3ijfbcA6E0kI8RW564ewixet'
access_token_secret='IgTNyw5ef932I1oYH3xv1xlyopZZq2HRSRkjTrPtLeYIr'

auth = tweepy.OAuthHandler('yXPhSCGufdwT0RfI2uwKNrg5w', 'nfJ4VK8IOT691PojlQ6qS5XoeOqXuaiirzj68iUYYjenBCpNDA')
auth.set_access_token('208354086-hLd2w2tcnl4BGrSM3ijfbcA6E0kI8RW564ewixet', 'IgTNyw5ef932I1oYH3xv1xlyopZZq2HRSRkjTrPtLeYIr')

# api = tweepy.API(auth)
# print(api.me().name)

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)

auth.set_access_token(access_token_key, access_token_secret)

api = tweepy.API(auth)

# keyword = "파이썬 OR 아는형님"
# location = "%s,%s,%s" % ("35.95", "128.25", "1000km")  # 검색기준(대한민국 중심) 좌표, 반지름
# cursor = tweepy.Cursor(api.search,
#                        q=keyword,
#                        since='2015-01-01', # 2015-01-01 이후에 작성된 트윗들로 가져옴
#
#                        count=100,  # 페이지당 반환할 트위터 수 최대 100
#
#                        geocode=location,
#
#                        include_entities=True)
#
# for i, tweet in enumerate(cursor.items()):
#     print("{}: {}".format(i, tweet.text))

from tweepy import OAuthHandler, Stream, StreamListener

class MyStreamLstener(tweepy.StreamListener):  #기존 tweepy의 streamListener의 오버라이딩
    def on_status(self, status):
        dataStr = status.text
        print (dataStr)  #데이터가 크롤링 되는 모습을 확인하기 위한 출력문

    def on_error(self, status_code):
        if status_code == 420:  #stream에 연결을 하지 못하는 에러가 발생하는 경우 False를 반환
            return False

if __name__ == '__main__':
    myStreamListener = MyStreamLstener()
    myStream = tweepy.Stream(auth=api.auth, listener=myStreamListener)
    myStream.filter(track=[u' '])
