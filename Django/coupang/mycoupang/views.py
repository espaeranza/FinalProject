# from django.shortcuts import render, redirect, get_object_or_404
# from django.views.decorators.http import require_http_methods, require_POST, require_safe
# from django.contrib.auth.decorators import login_required

# # Create your views here.
# @login_required
# def mycoupang(request):
#     return render(request, 'mycoupang/mycoupang.html')

# @require_safe
# def rating_predict(request):
#     return render(request, 'mycoupang/rating_predict.html') 

from django.shortcuts import render, redirect, get_object_or_404
from django.views.decorators.http import require_http_methods, require_POST, require_safe
from django.contrib.auth.decorators import login_required
from .forms import ArticleForm
import pandas as pd
from django.http import JsonResponse
from mycoupang.KobertModule.classifier import predict
# from .modules.data_anal import predict


# Create your views here.
@login_required
def mycoupang(request):
    customers = pd.read_csv('media/customers10_data.csv')
    personalized = []
    customers.rename(columns={'리뷰 제목': '제목', '리뷰 내용': '내용'}, inplace=True)
    
    for idx, review in customers.iterrows():
        if review['userid'] == str(request.user):
            personalized.append(review)
        
    return render(request, 'mycoupang/mycoupang.html', {'personalized': personalized})

@require_http_methods(["GET", "POST"])
def rating_predict(request):
    if request.method == 'GET':
        form = ArticleForm()  # input tag 대신 생성
    
    elif request.method == 'POST':
        form = ArticleForm(data=request.POST)

    return render(request, 'mycoupang/rating_predict.html', {
        'form': form,
    })

@require_http_methods(["GET", "POST"])
def rating_predict_ajax(request):
    if request.method == 'POST':
        form = ArticleForm(request.POST)
        title = request.POST['title'] # 사용자가 요청 시 전달해준 form data추출
        content = request.POST['content']
        
        predict_result = predict(content)
        # predict_result = json.dumps(predict_result, ensure_ascii = False)
        print(predict_result)
        return JsonResponse({"predict_result" :predict_result})
    
    else:    
        return render(request, 'mycoupang/rating_predict.html', {"predict_result" : None}, {'form': form})

def new_review(request):
    if request.method == 'GET':
        form = ArticleForm()  # input tag 대신 생성
    
    elif request.method == 'POST':
        form = ArticleForm(data=request.POST)

    return render(request, 'mycoupang/new_review.html', {
        'form': form,
    })

@require_http_methods(["GET", "POST"])
def new_review_ajax(request):
    if request.method == 'POST':
        form = ArticleForm(request.POST)
        title = request.POST['title'] # 사용자가 요청 시 전달해준 form data추출
        content = request.POST['content']
        
        predict_result = predict(content)
        # predict_result = json.dumps(predict_result, ensure_ascii = False)
        print(predict_result)
        return JsonResponse({"predict_result" :predict_result})
    
    else:    
        return render(request, 'mycoupang/rating_predict.html', {"predict_result" : None}, {'form': form})