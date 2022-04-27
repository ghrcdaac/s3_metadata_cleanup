from peewee import *
from playhouse.apsw_ext import APSWDatabase

SQLITE_VAR_LIMIT = 999
db = APSWDatabase(None, vfs='unix-excl')


def initialize_db(db_file_path):
    db.init(db_file_path, timeout=60, pragmas={
        'journal_mode': 'wal',
        'cache_size': -1 * 64000})
    db.create_tables([Metadata], safe=True)
    return db


class Metadata(Model):
    """
    Model representing a granule and the associated metadata
    """
    base_name = CharField(primary_key=True)
    xml_exists = BooleanField()
    json_exists = BooleanField()
    json_file_size = CharField()

    class Meta:
        database = db

    @staticmethod
    def select_all(granule_dict):
        """
        Selects all records from the database that are an exact match for all three fields
        :param granule_dict: Dictionary containing granules.
        :return ret_lst: List of granule names that existed in the database
        """
        ret_lst = []
        fields = [Metadata.base_name, Metadata.xml_exists, Metadata.json_exists]
        for key_batch in chunked(granule_dict, SQLITE_VAR_LIMIT // len(fields)):
            names = set()
            etags = set()
            last_mods = set()

            for key in key_batch:
                names.add(key)
                etags.add(granule_dict[key]["ETag"])
                last_mods.add(granule_dict[key]["Last-Modified"])

            sub = Metadata\
                .select(Metadata.base_name)\
                .where(Metadata.base_name.in_(names) & Metadata.xml_exists.in_(etags) & Metadata.json_exists.in_(last_mods))
            for name in sub.tuples().iterator():
                ret_lst.append(name[0])

        return ret_lst

    def db_skip(self, granule_dict):
        """
        Inserts all the granules in the granule_dict unless they already exist
        :param granule_dict: Dictionary containing granules.
        """
        for name in self.select_all(granule_dict):
            granule_dict.pop(name)
        return self.__insert_many(granule_dict)

    def db_replace(self, granule_dict):
        """
        Inserts all the granules in the granule_dict overwriting duplicates if they exist
        :param granule_dict: Dictionary containing granules.
        """
        return self.__insert_many(granule_dict)

    def db_error(self, granule_dict):
        """
        Tries to insert all the granules in the granule_dict erroring if there are duplicates
        :param granule_dict: Dictionary containing granules
        """
        fields = [Metadata.base_name]
        for key_batch in chunked(granule_dict, SQLITE_VAR_LIMIT // len(fields)):
            names = set()
            for key in key_batch:
                names.add(key)
            res = Metadata.select(Metadata.base_name).where(Metadata.base_name.in_(names))
            if res:
                raise ValueError('Granule already exists in the database.')

        return self.__insert_many(granule_dict)

    @staticmethod
    def delete_granules_by_names(granule_names):
        """
        Removes all granule records from the database if the name is found in granule_names.
        :return del_count: The number of deleted granules
        """
        del_count = 0
        for key_batch in chunked(granule_names, SQLITE_VAR_LIMIT):
            d = Metadata.delete().where(Metadata.base_name.in_(key_batch)).execute()
            del_count += d
        return del_count

    @staticmethod
    def __insert_many(granule_dict):
        """
        Helper function to separate the insert many logic that is reused between queries
        :param granule_dict: Dictionary containing granules
        """
        records_inserted = 0
        data = [(k, v['ETag'], v['Last-Modified']) for k, v in granule_dict.items()]
        fields = [Metadata.base_name, Metadata.xml_exists, Metadata.json_exists]
        with db.atomic():
            for key_batch in chunked(data, SQLITE_VAR_LIMIT // len(fields)):
                num = Metadata.insert_many(key_batch, fields=[Metadata.base_name, Metadata.xml_exists, Metadata.json_exists])\
                    .on_conflict_replace().execute()
                records_inserted += num

        return records_inserted


if __name__ == '__main__':
    pass
