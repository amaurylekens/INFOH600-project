import numpy as np
import matplotlib.pyplot as plt


class MonthlyDatum:
    def __init__(self, datum, year, month):
        self.datum = datum
        self.year = year
        self.month = month


class MonthlyDataBucket():
    def __init__(self, bucket_label, data_label, data=[]):
        self.bucket_label = bucket_label
        self.data_label = data_label

        if type(data) == list:
            self.data = data
        else:
            self.data = [data]

    def add_data(self, data):
        self.data.append(data)

        return self

    def get_data(self):
        return self.data

    def sort_by_date(self):
        self.data = sorted(self.data, 
                           key=lambda i: (i.year, 
                                          i.month))
        
        return self

    def plot(self, path='.'):
        
        # get x and y axis
        x_axis = np.array(["{}/{}".format(a.month, a.year)
                           for a in self.data])
        y_axis = np.array([a.datum for a in self.data])

        # plot
        fig = plt.figure()
        ax = fig.add_subplot(1, 1, 1) 
        ax.plot(x_axis, y_axis)
        ax.set_title('Monthly total {} for {} dataset'.format(self.data_label,
                                                              self.bucket_label))
        ax.set_ylabel(self.data_label)
        ax.set_xlabel('Month')
        #ax.set_xticklabels(ax.get_xticklabels(), rotation=45)
        fig.autofmt_xdate()
        fig.savefig('{}/{}.png'.format(path, self.bucket_label), format='png')


    @staticmethod
    def mix_buckets(b1, b2):
        data = b1.get_data() + b2.get_data()
        bucket = MonthlyDataBucket(b1.bucket_label, 
                                   b1.data_label, 
                                   data)

        return bucket

