import googleapiclient.discovery
import pandas as pd
import psycopg2
import sqlalchemy
from sqlalchemy import create_engine,DateTime,Interval
import streamlit as st

api_service_name = "youtube"
api_version = "v3"
api_key="AIzaSyC95-GH_6efmOjP0NEDyOWKg39wfbqe1yA"
youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=api_key)




with st.sidebar:
    st.title(":red[YOUTUBE DATA HAVERSTING AND WAREHOUSING]")
    

channel_id=st.text_input("Enter the channel ID")

if st.button("collect and store data"):
    #get channels information
    def get_channel_data(channel_id):

        request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id
        )
        response = request.execute()
        channel_list=[]
        for i in response["items"]:
            Channel_Name=i["snippet"]["title"],
            Channel_Id=i["id"],
            Subscribers=i['statistics']['subscriberCount'],
            Views=i["statistics"]["viewCount"],
            Total_Videos=i["statistics"]["videoCount"],
            Channel_Description=i["snippet"]["description"],
            Playlist_Id=i["contentDetails"]["relatedPlaylists"]["uploads"]
            channel_list.append([Channel_Name,Channel_Id,Subscribers,Views,Total_Videos,Channel_Description,Playlist_Id])
        df1=pd.DataFrame(channel_list,columns=['Channel_Name','Channel_Id','Subscribers','Views','Total_Videos','Channel_Description','Playlist_Id'])
        return df1

    ch_detail=get_channel_data(channel_id)
    
    #get video ids
    def get_videos_ids(channel_id):
        video_ids=[]
        response=youtube.channels().list(id=channel_id,
                                        part='contentDetails').execute()
        Playlist_Id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

        next_page_token=None

        while True:
            response1=youtube.playlistItems().list(
                                                part='snippet',
                                                playlistId=Playlist_Id,
                                                maxResults=50,
                                                pageToken=next_page_token).execute()
            for i in range(len(response1['items'])):
                video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
            next_page_token=response1.get('nextPageToken')

            if next_page_token is None:
                break
        return video_ids

    vi_id=get_videos_ids(channel_id)
    

    #get video information
    def get_video_info(video_ids):

        video_data=[]
        for video_id in video_ids:

            request=youtube.videos().list(
                    part="snippet,ContentDetails,statistics",
                    id=video_id
            )
            response=request.execute()

            for item in response["items"]:

                Channel_Name=item['snippet']['channelTitle'],
                Channel_Id=item['snippet']['channelId'],
                Video_Id=item['id'],
                Title=item['snippet']['title'],
                Tags=item['snippet'].get('tags'),
                Thumbnail=item['snippet']['thumbnails']['default']['url'],
                Description=item['snippet'].get('description'),
                Published_Date=item['snippet']['publishedAt'],
                Duration=item['contentDetails']['duration'],
                Views=item['statistics'].get('viewCount'),
                Likes=item['statistics'].get('likeCount'),
                Comments=item['statistics'].get('commentCount'),
                Favorite_Count=item['statistics']['favoriteCount'],
                Definition=item['contentDetails']['definition'],
                Caption_Status=item['contentDetails']['caption']
                video_data.append([Channel_Name,Channel_Id,Video_Id,Title,Tags,Thumbnail,
                                    Description,Published_Date,Duration,
                                    Views,Likes,Comments,Favorite_Count,Definition,Caption_Status])
        df2=pd.DataFrame(video_data,columns=['Channel_Name','Channel_Id','Video_Id','Title','Tags','Thumbnail',
                                        'Description','Published_Date','Duration',
                                        'Views','Likes','Comments','Favorite_Count','Definition','Caption_Status'])

        return df2

    video_information=get_video_info(vi_id)
       

    #get comment information
    def get_comment_info(video_ids):
        Comment_data=[]
        try:
            for video_id in video_ids:
                request=youtube.commentThreads().list(
                    part="snippet",
                    videoId=video_id,
                    maxResults=50
                )
                response=request.execute()

                for item in response['items']:

                    Comment_Id=item['snippet']['topLevelComment']['id'],
                    Video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                    Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                    Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    Comment_Published=item['snippet']['topLevelComment']['snippet']['publishedAt']

                    Comment_data.append([Comment_Id,Video_Id,Comment_Text,Comment_Author,Comment_Published])
        except:
            pass
        df3=pd.DataFrame(Comment_data,columns=['Comment_Id','Video_Id','Comment_Text',
                                                'Comment_Author','Comment_Published'])
        return df3

    comment=get_comment_info(vi_id)
    

    #get_playlist_details

    def get_playlist_details(channel_id):

        next_page_token=None
        All_data=[]
        while True:
                request=youtube.playlists().list(
                        part='snippet,contentDetails',
                        channelId=channel_id,
                        maxResults=50,
                        pageToken=next_page_token
                )
                response=request.execute()

                for item in response['items']:
                    Playlist_Id=item['id'],
                    Title=item['snippet']['title'],
                    Channel_Id=item['snippet']['channelId'],
                    Channel_Name=item['snippet']['channelTitle'],
                    PublishedAt=item['snippet']['publishedAt'],
                    Video_Count=item['contentDetails']['itemCount']
                    All_data.append([Playlist_Id,Title,Channel_Id,Channel_Name,PublishedAt,Video_Count])

                next_page_token=response.get('nextPageToken')
                if next_page_token is None:
                        break
        df4=pd.DataFrame(All_data,columns=['Playlist_Id','Title','Channel_Id','Channel_Name','PublishedAt','Video_Count'])
        return df4

    playlist_details=get_playlist_details(channel_id)
    
    st.dataframe(ch_detail)
    
#Table creation and uploads   
    host="localhost"
    user="postgres"
    password="Josunny"
    database="youtube1"
    port="5432"

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')
            

    ch_detail.to_sql(con=engine,name='channels',if_exists='append',index=False,
                    dtype={'Channel_Name':sqlalchemy.types.VARCHAR(length=225),
                            'Channel_Id':sqlalchemy.types.VARCHAR(length=225),
                            'Subscribers':sqlalchemy.types.BigInteger,
                            'Views':sqlalchemy.types.INT,
                            'Total_Videos':sqlalchemy.types.BigInteger,
                            'Channel_Description':sqlalchemy.types.TEXT,
                            'Playlist_Id':sqlalchemy.types.VARCHAR(length=225)})
            

    playlist_details.to_sql(con=engine,name='playlist', if_exists='append', index=False,
            dtype={"pl_Id": sqlalchemy.types.VARCHAR(length=225),
                    "channel_name": sqlalchemy.types.VARCHAR(length=225),
                    "channel_id": sqlalchemy.types.VARCHAR(length=225),
                    "publish_at": sqlalchemy.types.String(length=50),
                    "videos_count": sqlalchemy.types.INT})


    comment.to_sql(con=engine, name='comments', if_exists='append', index=False,
                    dtype={'Video_Id': sqlalchemy.types.VARCHAR(length=225),
                            'Comment_Id': sqlalchemy.types.VARCHAR(length=225),
                            'Comment_Text': sqlalchemy.types.TEXT,
                            'Comment_Author': sqlalchemy.types.VARCHAR(length=225),
                            'Comment_Published': sqlalchemy.types.String(length=50), })


    video_information.to_sql(con=engine, name='video',if_exists='append',index=False,
                    dtype={'Channel_name':sqlalchemy.types.VARCHAR(length=225),                       
                            'Channel_Id':sqlalchemy.types.VARCHAR(length=225),
                            'Video_id':sqlalchemy.types.VARCHAR(length=225),
                            'Title':sqlalchemy.types.VARCHAR(length=225),
                            'Tags':sqlalchemy.types.VARCHAR(length=2000),
                            'Thumbnail':sqlalchemy.types.VARCHAR(length=225),
                            'Description':sqlalchemy.types.VARCHAR(length=8000),
                            'Published_date':sqlalchemy.types.Date,
                            'Duration':sqlalchemy.types.Interval,
                            'Views':sqlalchemy.types.BigInteger,
                            'Likes':sqlalchemy.types.BigInteger,
                            'Comments':sqlalchemy.types.INT,
                            'Favorite_count':sqlalchemy.types.INT, 
                            'Caption_status':sqlalchemy.types.VARCHAR(length=225)})
    engine.dispose()

    st.success('**Successfully uploaded in DATABASE**')


# TO VIEW ALL THE UPLOADED CHANNELS     
Check_channel = st.checkbox('**Check available channel data for analysis**')
if Check_channel:
    mydb=psycopg2.connect(host="localhost",
                    user="postgres",
                    password="Josunny",
                    database="youtube1",
                    port="5432")
    cursor=mydb.cursor()
    cursor.execute('''select "Channel_Name","Channel_Id" from public.channels ''')
    results= cursor.fetchall()
    channel_names_fromsql = list(results)    
    df_at_sql = pd.DataFrame(channel_names_fromsql)
    df_at_sql.drop_duplicates(inplace=True)
    df_at_sql.index += 1
    st.dataframe(df_at_sql)


#Queries
st.write("QUREY OUTPUT")

question_tosql = st.selectbox('Welcome to Q & A',
                                ('Select Question',
                                '1. What are the names of all the videos and their corresponding channels?',
                                '2. Which channels have the most number of videos, and how many videos do they have?',
                                '3. What are the top 10 most viewed videos and their respective channels?',
                                '4. How many comments were made on each video, and what are their corresponding video names?',
                                '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
                                '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
                                '7. What is the total number of views for each channel, and what are their corresponding channel names?',
                                '8. What are the names of all the channels that have published videos in the year 2022?',
                                '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
                                '10. Which videos have the highest number of comments, and what are their corresponding channel names?'),
                                key='collection_question')

# Create a connection to SQL
mydb=psycopg2.connect(host="localhost",
                    user="postgres",
                    password="Josunny",
                    database="youtube1",
                    port="5432")
cursor=mydb.cursor()


if question_tosql == '1. What are the names of all the videos and their corresponding channels?':
    cursor.execute('''
            SELECT "Channel_Name","Title" from public.video''')
    result_1 = cursor.fetchall()
    df_1 = pd.DataFrame(result_1, columns=['Channel Name', 'Video Name'])
    df_1.index += 1
    st.dataframe(df_1)

# Q2
elif question_tosql == '2. Which channels have the most number of videos, and how many videos do they have?':
    cursor.execute('''SELECT "Channel_Name","Total_Videos"  FROM public.channels ORDER BY "Total_Videos" DESC;''')
    result_2 = cursor.fetchall()
    df_2 = pd.DataFrame(result_2, columns=['Channel Name', 'Video Count'])
    df_2.index += 1
    st.dataframe(df_2)


# Q3
elif question_tosql == '3. What are the top 10 most viewed videos and their respective channels?':
    cursor.execute('''
        SELECT "Title", "Views","Channel_Name" FROM public.video ORDER BY "Views" DESC LIMIT 10;''')
    result_3 = cursor.fetchall()
    df_3 = pd.DataFrame(result_3, columns= ['Video Name', 'View count','Channel Name'])
    df_3.index += 1
    st.dataframe(df_3)

    

# Q4
elif question_tosql == '4. How many comments were made on each video, and what are their corresponding video names?':
    cursor.execute('''SELECT "Title", "Comments" FROM public.video;''')
    result_4 = cursor.fetchall()
    df_4 = pd.DataFrame(result_4, columns=['Video Name', 'Comment count'])
    df_4.index += 1
    st.dataframe(df_4)

# Q5
elif question_tosql == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
    cursor.execute(
        '''SELECT "Channel_Name", "Title","Likes" FROM video ORDER BY "Likes" DESC;''')
    result_5 = cursor.fetchall()
    df_5 = pd.DataFrame(result_5, columns=['Channel Name', 'Video Name', 'Like count'])
    df_5.index += 1
    st.dataframe(df_5)

# Q6
elif question_tosql == '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
    
    cursor.execute('''
        SELECT "Channel_Name", "Title","Likes" FROM public.video ORDER BY "Likes" DESC;''')
    result_6 = cursor.fetchall()
    df_6 = pd.DataFrame(result_6, columns=['Channel Name', 'Video Name', 'Like count', ])
    df_6.index += 1
    st.dataframe(df_6)

# Q7
elif question_tosql == '7. What is the total number of views for each channel, and what are their corresponding channel names?':

    cursor.execute('''SELECT "Channel_Name","Views" FROM public.channels ORDER BY "Views" DESC;''')
    result_7 = cursor.fetchall()
    df_7 = pd.DataFrame(result_7, columns=['Channel Name', 'Total number of views'])
    df_7.index += 1
    st.dataframe(df_7)
# Q8
elif question_tosql == '8. What are the names of all the channels that have published videos in the year 2022?':
    cursor.execute('''
        SELECT "Channel_Name", "Published_Date" FROM public.video WHERE EXTRACT(YEAR FROM "Published_Date"::date)= 2022;''')
    result_8 = cursor.fetchall()
    df_8 = pd.DataFrame(result_8, columns=['Channel Name', 'Year 2022 only'])
    df_8.index += 1
    st.dataframe(df_8)

    # Q9
elif question_tosql == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
    cursor.execute('''
            SELECT "Channel_Name", AVG("Duration") AS avgduration  FROM public.video GROUP BY "Channel_Name" ;''')
    result_9 = cursor.fetchall()
    df_9 = pd.DataFrame(result_9, columns=['Channel Name', 'Average duration of videos (HH:MM:SS)'])
    df_9.index += 1
    st.dataframe(df_9)

# # Q10
elif question_tosql == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
    cursor.execute('''
        SELECT "Channel_Name", "Title", "Comments" FROM public.video ORDER BY "Comments" DESC;''')
    result_10 = cursor.fetchall()
    df_10 = pd.DataFrame(result_10, columns=['Channel Name', 'Video Name', 'Number of comments'])
    df_10.index += 1
    st.dataframe(df_10)

