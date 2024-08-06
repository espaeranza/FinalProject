# mycoupang/forms.py
from django import forms
from .models import Article

class ArticleForm(forms.ModelForm):
    # 0.(fields에 적힌 컬럼에 대해서만)
    # 1. 입력 데이터 검증
    # 2. HTML(input/textarea/...)을 생성
    title = forms.CharField(min_length=2, max_length=40,
                            widget = forms.TextInput(attrs={'class': 'form-control'}))
    content = forms.CharField(
        min_length=5,
        # forms.CharField는 기본값이 input type text
        # widget을 통해 HTML 코드로 바꿀 수 있음
        widget = forms.Textarea(
            # class 주는 코드 예시
            attrs={'class': 'form-control'}
        )
    )

    class Meta:
        model = Article
        fields = '__all__' # 모든 field 대해서 생성
        # fields = ('content', ) 지정한 부분만 생성됨