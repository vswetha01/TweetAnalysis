from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import boto3
import boto3.s3
import json
import botocore


AWS_Access_Key_ID = "XXXXXXX"
AWS_Secret_Access_Key = "XXXXXXX"

# Twitter Crendentials
# Twitter App Name: "pink_eye_tracker"
access_token = "XXXXXX"
access_token_secret = "XXXXX"

consumer_key = "XXXXXX"
consumer_secret = "XXXXX"

tweets = []
index = 0
# This is a basic listener that just prints received tweets to stdout.
class StdOutListener(StreamListener):

    @staticmethod
    def write_to_s3_bucket(data_tweets):
        """
        Method to write to s3 bucket
        :param self:
        :param data_tweets: list of tweet tuple
        :return:
        """
        s3 = boto3.resource('s3', aws_access_key_id=AWS_Access_Key_ID, aws_secret_access_key=AWS_Secret_Access_Key)
        all_tweets = []
        global index
        index += 1
        fn = "examples/file_"+ str(index)+".csv"
        print "writing 5 tweets"

        for twt in data_tweets:
            str_tweets = ",".join([x or '' for x in twt])
            all_tweets.append(str_tweets)

        all_twt = "\n".join(all_tweets)

        t_bucket = s3.Bucket('tweetBucket')
        exists = True
        try:
            s3.meta.client.head_bucket(Bucket='tweetBucket')
        except botocore.exceptions.ClientError as e:
            # If a client error is thrown, then check that it was a 404 error.
            # If it was a 404 error, then the bucket does not exist.
            error_code = int(e.response['Error']['Code'])
            if error_code == 404:
                exists = False
        if exists is False:
            print "creating bucket"
            t_bucket = s3.create_bucket(Bucket='tweetBucket')

        key = s3.Object("tweetBucket", fn)
        key.put(Body=all_twt)
        t_bucket.Acl().put(ACL='public-read')
        print "done writing to bucket files %s" % fn
        return

    def on_data(self, data):
        """
        Event listener for reading the tweet data
        :param data: Json tweet input
        :return:
        """
        all_data = json.loads(data)

        if "id" in all_data:
            tweet_id = str(all_data['id'])
        else:
            tweet_id = None
        if 'created_at' in all_data:
            created_at = all_data['created_at']
        else:
            created_at = None
        if 'text' in all_data:
            tweet_txt = all_data['text']
        else:
            tweet_txt= None
        # read the rest as needed

        # tweet_txt = re.sub("'", "", tweet_txt)  # remove ' from tweets; messes up the load into PSQL
        # geo = all_data['geo']
        # coords = all_data['coordinates']
        # place = all_data['place']
        tweets_tup = (tweet_id, created_at,tweet_txt)
        print tweets_tup
        global tweets
        tweets.append(tweets_tup)
        # todo: change this 1000 tweets atleast
        if len(tweets) == 5:
            self.write_to_s3_bucket(tweets)
            tweets = []
        return True

    def on_error(self, status_code):
        if status_code == 420:
            # returning False in on_data disconnects the stream
            return False


if __name__ == '__main__':

    # This handles Twitter authentication and the connection to Twitter Streaming API
    l = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, l)
    # # This line filters Twitter Streams to capture data by the keywords:
    stream.filter(track=['twitter'])
                  #['pink eye', 'pinkeye', 'conjunctivitis', 'keratoconjunctivitis', 'madras eye', 'ekc'])
