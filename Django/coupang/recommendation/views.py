from django.shortcuts import render
import pandas as pd
from django.contrib.auth.decorators import login_required

# Create your views here.
def mainpage(request):
    return render(request, 'recommendation/mainpage.html')
    
def category(request):
    return render(request, 'recommendation/category.html')


def category_recommendation(data):
    products = pd.read_csv('data/total.csv')
    products = products[['카테고리1', '카테고리2', '제품명', '리뷰 개수', '상품 별점']]
    products

    products_sample = products
    for idx, row in products_sample.iterrows():
        products_sample.loc[idx, '제품명'] = row['제품명'].split(',')[0]
    
        products = products_sample.drop_duplicates(subset='제품명', keep='first')
    
        second = products['카테고리2'].unique()
    second = [i.replace(' ', '') for i in second]

    for idx, row in data.iterrows():
        if (row['카테고리2'] not in second):
            data.drop([idx], axis=0, inplace=True)
    data = data.reset_index()

    data = data[['별점', '카테고리1', '카테고리2']]
    
    data_count = data[['별점', '카테고리1', '카테고리2']]
    data_count.rename(columns = {'별점': '리뷰 개수'}, inplace=True)
    
    rating = data.groupby(['카테고리1', '카테고리2']).mean().sort_values(by=['별점'], ascending=False)
    
    counts = data_count.groupby(['카테고리1', '카테고리2']).count().sort_values(by=['리뷰 개수'], ascending=False)
    
    rating_count = pd.concat([rating, counts], axis=1).sort_values(by=['리뷰 개수'], ascending=False)
    
    # 리뷰 개수: v
    # 평균 평점: r
    # 전체 리뷰에 대한 평균 별점: c
    # 가중 평점: v*r + v*c
    average_rating = rating_count['별점'].mean()
    rating_count['가중 평점'] = rating_count['리뷰 개수'] * rating_count['별점'] + rating_count['별점'] * average_rating
    rating_count = rating_count.sort_values(by=['가중 평점'], ascending=False)
    rating_count = rating_count.reset_index()
    
    top10_category = rating_count.loc[:9]

    return top10_category

# Create your views here.
#@login_required
def index(request):
    data = pd.read_csv('data/cust1.csv')
    top10_category = category_recommendation(data)
    top10 = []
    for i in range(10):
        top10.append([top10_category.loc[i]['카테고리1'], top10_category.loc[i]['카테고리2']])
    return render(request, 'recommendation/index.html',{'top10':top10})

