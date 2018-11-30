from pynamodb.models import Model
from pynamodb.attributes import UnicodeAttribute, UnicodeSetAttribute, MapAttribute


class TestModel(Model):
    class Meta:
        table_name = 'btc_transactions'
    hash = UnicodeAttribute(hash_key=True)
    value = UnicodeAttribute()
    friends = UnicodeSetAttribute()
    new_attr = MapAttribute(default={})
