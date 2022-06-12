import streamlit as st
import altair as alt
from pandas import DataFrame
import pymongo


myclient = pymongo.MongoClient("mongodb://localhost:27017/",username='root',password='example')
mydb = myclient["docstreaming"]
mycol = mydb["videos"]
cursor = mycol.find({})
full_df= DataFrame(cursor)

st.title("Welcome to BDA Project")
st.subheader("by Abeera Tariq -13170")
st.write("Use the side-bar to query on the streaming data")

button=st.sidebar.button("Visualization")
video_id = st.sidebar.text_input("Exact Video ID:")
regex_id = st.sidebar.text_input("Regex for Video ID:")


if video_id:

    myquery = {"video_id": video_id}
    # only includes or excludes
    mydoc = mycol.find( myquery , { "category_id": 0, "_id": 0})

    # create dataframe from resulting documents to use drop_duplicates
    df = DataFrame(mydoc)
    
    # drop duplicates, but keep the first one
    df.drop_duplicates(subset ="channel_title", keep = 'first', inplace = True)
    
    # Add the table with a headline
    st.header("Output Videos by ID")
    table2 = st.dataframe(data=df)

if regex_id:
    
    myquery = {"video_id": {"$regex": regex_id}}
    # only includes or excludes
    mydoc = mycol.find(myquery, { "_id": 0, "trending_date": 0, "views": 0, "likes": 0, "dislikes": 0, "comment_count": 0 })

    # create dataframe from resulting documents to use drop_duplicates
    df = DataFrame(mydoc)
    print(df)
    # drop duplicates, but keep the first one
    df.drop_duplicates(subset ="video_id", keep = 'first', inplace = True)
    
    # Add the table with a headline
    st.header("Output Videos by Regex for Video ID")
    table3 = st.dataframe(data=df)

title = st.sidebar.text_input("Exact Title:")
regex_title = st.sidebar.text_input("Regex for Title:")
if title:

    myquery = {"title": title}
    # only includes or excludes
    mydoc = mycol.find( myquery , { "category_id": 0, "_id": 0})

    # create dataframe from resulting documents to use drop_duplicates
    df = DataFrame(mydoc)
    
    # drop duplicates, but keep the first one
    df.drop_duplicates(subset ="channel_title", keep = 'first', inplace = True)
    
    # Add the table with a headline
    st.header("Output Videos by Title")
    table2 = st.dataframe(data=df)

if regex_title:
    
    myquery = {"title": {"$regex": regex_title}}
    # only includes or excludes
    mydoc = mycol.find(myquery, { "_id": 0, "trending_date": 0, "views": 0, "likes": 0, "dislikes": 0, "comment_count": 0 })

    # create dataframe from resulting documents to use drop_duplicates
    df = DataFrame(mydoc)
    print(df)
    # drop duplicates, but keep the first one
    df.drop_duplicates(subset ="title", keep = 'first', inplace = True)
    
    # Add the table with a headline
    st.header("Output Videos by Regex for Title")
    table3 = st.dataframe(data=df)




channel_title = st.sidebar.text_input("Exact Channel Title:")

if channel_title:
    
    myquery = {"channel_title": channel_title}
    mydoc = mycol.find( myquery, { "_id": 0, "trending_date": 0, "views": 0, "likes": 0, "dislikes": 0, "comment_count": 0 })

    # create the dataframe
    df = DataFrame(mydoc)

    # drop duplicates, but keep the first one
    df.drop_duplicates(subset ="video_id", keep = 'first', inplace = True)
    
    # Add the table with a headline
    st.header("Output by Channel Title")
    table2 = st.dataframe(data=df) 

regex_channel = st.sidebar.text_input("Regex for Channel Title:")

if regex_channel:
    
    myquery = {"channel_title": {"$regex": regex_channel}}
    # only includes or excludes
    mydoc = mycol.find(myquery, { "_id": 0, "trending_date": 0, "views": 0, "likes": 0, "dislikes": 0, "comment_count": 0 })

    # create dataframe from resulting documents to use drop_duplicates
    df = DataFrame(mydoc)
    print(df)
    # drop duplicates, but keep the first one
    df.drop_duplicates(subset ="channel_title", keep = 'first', inplace = True)
    
    # Add the table with a headline
    st.header("Output Videos by Regex for Channel Title")
    table3 = st.dataframe(data=df)

if button:
    

## WAS GIVING LOADING JAVA SCRIPT ERRORS
##    df = DataFrame(view_sort[0:10])
##    source = DataFrame({
##        'a': df["title"],
##        'b': df["views"]
##    })
##
##    alt.Chart(source).mark_bar().encode(
##        x='a',
##        y='b'
##    )

    st.header("Top 10 Trending viewed Youtube Videos")
    view_sort=mycol.find().sort( "views", pymongo.DESCENDING )
    
    df = DataFrame(view_sort[0:10])
    chart_data = DataFrame(
         df["views"])

    st.bar_chart(chart_data)

    st.header("Top 10 commented Trending Youtube Videos")
    view_sort=mycol.find().sort( "comment_count", pymongo.DESCENDING )

    df = DataFrame(view_sort[0:10])
    chart_data = DataFrame(
         df["comment_count"])

    st.bar_chart(chart_data)

    st.header("Count by Category_id")
    result= mycol.aggregate([
    {
        "$match": {
            "category_id": { "$not": {"$size": 0} }
        }
    },
    { "$unwind": "$category_id" },
    {
        "$group": {
            "_id": {"$toLower": '$category_id'},
            "count": { "$sum": 1 }
        }
    },
    {
        "$match": {
            "count": { "$gte": 2 }
        }
    },
    { "$sort" : { "count" : -1} },
    { "$limit" : 100 }
])

    df = DataFrame(result)
    table3 = st.dataframe(data=df)

    chart_data = DataFrame(
         df["_id"], df["count"])

    st.line_chart(chart_data)
    
    

