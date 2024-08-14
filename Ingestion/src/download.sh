
kaggle datasets download -d olistbr/brazilian-ecommerce
if [ -d data/ ]; then 
    unzip -o brazilian-ecommerce.zip -d data/
else
    mkdir data/
    unzip brazilian-ecommerce.zip -d data/
fi