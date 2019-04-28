import time
import datetime as dt
import os


def get_desktop_path():
    return os.path.join(os.path.expanduser("~"), 'Desktop')


def tic(string):
    print(dt.datetime.now(), "------", string)


# fund_weekly_index
def compare(t_min, t_std_m1):
    result = []
    for x in t_min:
        i = 0
        for y in t_std_m1:
            if x <= y:
                i += 1
            else:
                break
        result.append(i)
    return result


# fund_monthly_index
def merge_result(r1, r2, r3):
    result = r1[:]
    for i in range(len(result[1])):
        if result[1][i] is None:
            if r2[1][i] is not None:
                result[1][i] = r2[1][i]
                result[0][i] = r2[0][i]
            else:
                if r3[1][i] is not None:
                    result[1][i] = r3[1][i]
                    result[0][i] = r3[0][i]
                else:
                    continue
    return result


# 1y_treasury_rate
def annually2weekly(x):
    return (1 + x / 100) ** (1 / 52) - 1


def annually2monthly(x):
    return (1 + x / 100) ** (1 / 12) - 1


def annually2freq(x, freq):
    periods = {"w": 52, "m": 12, "d": 250}
    return (1 + x / 100) ** (1 / periods[freq]) - 1


def date2tstp(x):
    return time.mktime(x.timetuple())


def idx4slice(dataframe, slice_by="fund_id"):
    length = len(dataframe)
    dataframe = dataframe.drop_duplicates(slice_by)
    idxs = dataframe.index.tolist()
    idxs.append(length)
    return idxs


def slice(dataframe, idxs, col="nav"):
    df_sub = dataframe[col].tolist()
    return [df_sub[idxs[i]:idxs[i + 1]] for i in range(len(idxs) - 1)]
