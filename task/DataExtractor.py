import pandas as pd

class DataExtractor:
    def __init__(self, invoices_file, expired_invoices_file):
        self.invoices_path = invoices_file
        self.expired_invoices_path = expired_invoices_file

    def load_data(self):
        invoices = pd.read_pickle(self.invoices_path)
        self.invoices_path = pd.DataFrame(invoices)

        with open(self.expired_invoices_path, 'r') as f:
            self.expired_invoices_path = set(int(id.strip()) for line in f for id in line.split(','))

    def transform_data(self):
        transformed_data = []
        for _, row in self.invoices_path.iterrows():
            invoice_id = row['id']
            created_on = pd.to_datetime(row['created_on'], errors='coerce')
            invoice_total = 0

            if not isinstance(row['items'], float):
                for item in row['items']:
                    item['quantity'] = pd.to_numeric(item['quantity'], errors='coerce')
                    item['item']['unit_price'] = pd.to_numeric(item['item']['unit_price'], errors='coerce')
                    invoice_total += item['item']['unit_price'] * item['quantity']

            if not isinstance(row['items'], float):
                for item in row['items']:
                    invitem_id = item['item']['id']
                    inv_name = item['item']['name']
                    types = {0: 'Material', 1: 'Equipment', 2: 'Service', 3: 'Other'}
                    type_str = types.get(item['item']['type'], 'Other')
                    unit_price = item['item']['unit_price']
                    quantity = item['quantity']
                    total_price = unit_price * quantity
                    percentage_in_invoice = (total_price / invoice_total) if invoice_total != 0 else 0
                    is_expired = invoice_id in self.expired_invoices_path

                    transformed_data.append({
                        'invoice_id': invoice_id,
                        'created_on': created_on,
                        'invoiceitem_id': invitem_id,
                        'invoiceitem_name': inv_name,
                        'type': type_str,
                        'unit_price': unit_price,
                        'quantity': quantity,
                        'total_price': total_price,
                        'percentage_in_invoice': percentage_in_invoice,
                        'is_expired': is_expired
                    })

                    df = pd.DataFrame(transformed_data)
                    df["invoiceitem_id"] = pd.to_numeric(df["invoiceitem_id"], errors="coerce").astype("Int64")
                    df["total_price"] = pd.to_numeric(df["total_price"], errors="coerce").astype("Int64")
                    df = df.sort_values(by=['invoice_id', 'invoiceitem_id']).reset_index(drop=True)

        return df
