mkdir -p ~/.kaggle
aws s3 cp "s3://timepassbucket-flipkart/api_kaggle/kaggle.json" ~/.kaggle/
chmod 600 ~/.kaggle/kaggle.json
curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
python3 get-pip.py
pip install boto3
pip install Kaggle
pip install csvkit
aws s3 cp "s3://timepassbucket-flipkart/api_kaggle/download_data.py" .
chmod 777 download_data.py
python3 download_data.py
aws s3 cp dim_product.csv "s3://bucket-uncleaned-flipkart/folder_for_dim_flipkart/"
csvstack fact_sales_apr1.csv fact_sales_apr2.csv fact_sales_may1.csv fact_sales_may2.csv fact_sales_jun1.csv fact_sales_jun2.csv fact_sales_jul1.csv > fact_sales.csv
aws s3 cp fact_sales.csv "s3://bucket-uncleaned-flipkart/folder_for_fact_flipkart/"
