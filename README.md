现在文档都是在本地生成，还没有开放出来

文档生成流程：

```bash
python setup.py install
cd docs
pip install -r requirements.txt
make html
```
然后用浏览器打开`docs/build/html/index.html`这个页面就好了