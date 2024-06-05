from DataExtractor import DataExtractor

invoices_file = '/Users/diananersesyan/Desktop/ST24intern/data/invoices_new.pkl'
expired_invoices_file = '/Users/diananersesyan/Desktop/ST24intern/data/expired_invoices.txt'

ext = DataExtractor(invoices_file, expired_invoices_file)
ext.load_data()
transform = ext.transform_data()


print(transform.dtypes)
print(transform.head())


