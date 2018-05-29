import os
import pandas as pd
from .utils import ReaderType


def safe_tagname(tagname):
    tagname = tagname.replace('.', '_')
    tagname = "".join(c for c in tagname if c.isalnum() or c == '_').strip()
    if tagname[0].isnumeric():
        tagname = '_' + tagname  # Conform to NaturalName
    return tagname


class SmartCache():
    def __init__(self, filename, path='.'):
        self.filename = os.path.splitext(filename)[0] + '.h5'
        self.filename = os.path.join(path, self.filename)
        #self.open(self.filename)

    # def open(self, filename=None):
    #     if filename is None:
    #         filename = self.filename
    #     self.hdfstore = pd.HDFStore(filename)
    #
    # def close(self):
    #     self.hdfstore.close()


    def key_path(self, df, readtype, ts=None):
        """Return a string on the form
        XXX/sYY/ZZZ where XXX is the ReadType, YY is the interval between samples (in seconds)
        and ZZZ is a safe tagname.
        """
        name = list(df)[0] if isinstance(df, pd.DataFrame) else df
        name = safe_tagname(name)
        ts = ts.seconds if isinstance(ts, pd.Timedelta) else ts
        if readtype != ReaderType.RAW:
            if ts is None:
                # Determine sample tamie by reading interval between first two samples of dataframe.
                if isinstance(df, pd.DataFrame):
                    interval = int(df[0:2].index.to_series().diff().mean().value/1e9)
                else:
                    raise TypeError
            else:
                interval = int(ts)
            return f'{readtype.name}/s{interval}/{name}'
        else:
            return f'{readtype.name}/{name}'

    def store(self, df, readtype, ts=None):
        key = self.key_path(df, readtype, ts)
        with pd.HDFStore(self.filename, mode='a') as f:
            if key in f:
                idx = f.select(key, where="index in df.index", columns=['index']).index
                f.append(key, df.query("index not in @idx"))
            else:
                f.append(key, df)

    def fetch(self, tagname, readtype, ts=None, start_time=None, stop_time=None):
        df = pd.DataFrame()
        if not os.path.isfile(self.filename):
            return df
        key = self.key_path(tagname, readtype, ts)
        where = []
        if start_time is not None:
            where.append("index >= start_time")
        if stop_time is not None:
            where.append("index < stop_time")
        where = " and ".join(where)
        with pd.HDFStore(self.filename, mode='r') as f:
            if key in f:
                if where:
                    df = f.select(key, where=where)
                else:
                    df = f.select(key)
        return df

    def remove(self, filename=None):
        if not filename:
            filename = self.filename
            #self.close()
        if os.path.isfile(filename):
            os.unlink(filename)

    def _match_tag(self, key, readtype=None, ts=None, tagname=None):
        def readtype_to_str(rt):
            return getattr(rt, 'name', rt) # if isinstance(rt, ReaderType) always returns False...?

        def timedelta_to_str(t):
            if isinstance(t, pd.Timedelta):
                return str(t.seconds)
            return t
        key = '/'+key.lstrip('/') # Ensure absolute path
        readtype = readtype if isinstance(readtype, list) else [readtype]
        ts = ts if isinstance(ts, list) else [ts]
        tagname = tagname if isinstance(tagname, list) else [tagname]
        readtype = list(map(readtype_to_str, readtype))
        ts = list(map(timedelta_to_str, ts))
        if tagname[0] is not None:
            tagname = list(map(safe_tagname, tagname))
        #print(f"Readtype: {readtype}, ts: {ts}, tagname: {tagname}")
        elements = key.split('/')[1:]
        if len(elements) == 2:
            elements.insert(1, None)
        else:
            elements[1] = int(elements[1][1:]) # Discard the initial s
        if elements[0] not in readtype and readtype[0] is not None:
            #print(f"{elements[0]} not in {readtype}")
            return False
        if elements[1] not in ts and ts[0] is not None:
            #print(f"{elements[1]} not in {ts}")
            return False
        if elements[2] not in tagname and tagname[0] is not None:
            #print(f"{elements[2]} not in {tagname}")
            return False
        return True

    def delete_key(self, tagname=None, readtype=None, ts=None):
        with pd.HDFStore(self.filename) as f:
            for key in f:
                if self._match_tag(key, tagname=tagname, readtype=readtype, ts=ts):
                    f.remove(key)

    def _get_hdfstore(self, mode='r'):
        f = pd.HDFStore(self.filename, mode)
        return f
