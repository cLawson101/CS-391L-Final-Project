import numpy as np
import pandas as pd
from datetime import datetime
import pyarrow.parquet as pq
import matplotlib.pyplot as plt
from sklearn.model_selection import train_test_split
from sklearn.metrics import r2_score
import warnings
import argparse

# Torch required packages
import torch
import torch.nn as nn
from torch import Tensor
import torch.nn.functional as F
from torch.utils.data import Dataset, DataLoader

def parse_args():
    parser = argparse.ArgumentParser()
    # parser.add_argument("--data_path", required=True, type = str)
    parser.add_argument("--max_epochs", type=int, default = 10)

    
    # Model Stuff
    parser.add_argument("--training_length", type=int, default = 24)
    parser.add_argument("--forecast_window", type=int, default = 24)
    parser.add_argument("--num_layers", type=int, default = 1)
    parser.add_argument("--num_heads", type=int, default = 1)
    parser.add_argument("--dropout", type=float, default = 0.0)
    parser.add_argument("--batch_size", type=int, default = 1)

    # Optimizer Stuff
    parser.add_argument('--lr', type=float, default=1e-4) # 8e-4
    
    config = parser.parse_args().__dict__
    return config

class TimeSeriesDataset(Dataset):
    def __init__(self, raw_data, raw_label, training_length, forecast_window):
        
        df = raw_data.copy()
        df["SOUTH_C"] = raw_label
        
        self.df = df
        self.T = training_length
        self.S = forecast_window
        
    def __len__(self):
        return len(self.df)
    
    def __getitem__(self, idx):
        training_items = self.df[['DRY_BULB_TEMPERATURE_KAUS',
                             'RELATIVE_HUMIDITY_KAUS',
                             'DRY_BULB_TEMPERATURE_KSAT',
                             'RELATIVE_HUMIDITY_KSAT',
                             'SOUTH_C']]
        
        testing_items = self.df[["SOUTH_C"]]
        
        _input = training_items[idx : idx + self.T].values
        target = testing_items[idx + self.T : idx + self.T + self.S].values
        return _input, target

class Transformer(nn.Module):
    # d_model : number of features
    def __init__(self,feature_size=5,num_layers=3,dropout=0,nheads=5):
        super(Transformer, self).__init__()

        self.encoder_layer = nn.TransformerEncoderLayer(d_model=feature_size, nhead=nheads, dropout=dropout)
        self.transformer_encoder = nn.TransformerEncoder(self.encoder_layer, num_layers=num_layers)        
        self.decoder = nn.Linear(feature_size,1)
        self.init_weights()

    def init_weights(self):
        initrange = 0.1    
        self.decoder.bias.data.zero_()
        self.decoder.weight.data.uniform_(-initrange, initrange)

    def _generate_square_subsequent_mask(self, sz):
        mask = (torch.triu(torch.ones(sz, sz)) == 1).transpose(0, 1)
        mask = mask.float().masked_fill(mask == 0, float('-inf')).masked_fill(mask == 1, float(0.0))
        return mask.type(torch.BoolTensor)

    def forward(self, src):
        mask = self._generate_square_subsequent_mask(len(src))
        output = self.transformer_encoder(src, mask = mask)
        output = self.decoder(output)
        return output

if __name__ == "__main__":
    config = parse_args()
    south_central_wz = pq.read_table(f"data/south_central_wz.parquet").to_pandas()

    lim = south_central_wz[[
        'YEAR', 'MONTH', 'DAY', 'HOUR_ENDING', 
        'SOUTH_C',
        'DRY_BULB_TEMPERATURE_KAUS', 
        'RELATIVE_HUMIDITY_KAUS', 
        'DRY_BULB_TEMPERATURE_KSAT', 
        'RELATIVE_HUMIDITY_KSAT',
    ]]

    for str_issue_col in [
        'DRY_BULB_TEMPERATURE_KAUS', 
        'RELATIVE_HUMIDITY_KAUS', 
        'DRY_BULB_TEMPERATURE_KSAT', 
        'RELATIVE_HUMIDITY_KSAT'
    ]:
        with pd.option_context('mode.chained_assignment', None):
            lim[str_issue_col] = [_.replace('s', '') if isinstance(_, str) else _ for _ in lim[str_issue_col]]
            lim.loc[lim[str_issue_col] == '*', str_issue_col] = None
            lim[str_issue_col] = lim[str_issue_col].astype('float')
    
    lim_datetime = lim.copy()
    lim_datetime['DATETIME'] = [
        datetime(int(row['YEAR']), int(row['MONTH']), int(row['DAY']), int(row['HOUR_ENDING'] - 1))  # hour cannot be 24 
        for i, row in lim_datetime.iterrows()
    ]
    lim_datetime = lim_datetime[[_ for _ in lim_datetime.columns if _ not in ['YEAR', 'MONTH', 'DAY', 'HOUR_ENDING']]]
    lim_datetime.dropna(how='any', inplace=True)

    # TODO: check data load process to see if this limiation can be avoided
    lim = lim.dropna(how='any')
    with pd.option_context('mode.chained_assignment', None):
        datetime_ref = [
            datetime(int(row['YEAR']), int(row['MONTH']), int(row['DAY']), int(row['HOUR_ENDING'] - 1))  # hour cannot be 24 
            for i, row in lim.iterrows()
        ]
    
    cols = lim.columns.tolist()
    features = cols[0:4] + cols[5:]

    X = lim[features]
    y = lim[["SOUTH_C"]]
    train_X, test_X, train_y, test_y = train_test_split(
        X,
        y,
        test_size = 0.1
    )

    training_length = config["training_length"]
    forecast_window = config["forecast_window"]

    train_dataset = TimeSeriesDataset(train_X, train_y, training_length, forecast_window)
    train_dataloader = DataLoader(train_dataset, batch_size = config["batch_size"], shuffle = True)

    test_dataset = TimeSeriesDataset(test_X, test_y, training_length, forecast_window)
    test_dataloader = DataLoader(test_dataset, batch_size = config["batch_size"], shuffle = True)

    MAX_EPOCH = config["max_epochs"]
    FEATURES = 5
    NUM_LAYERS = config["num_layers"]
    DROPOUT = config["dropout"]
    LR = config["lr"]
    NUM_HEADS = config["num_heads"]

    model = Transformer(feature_size = FEATURES, 
                    num_layers = NUM_LAYERS,
                    dropout = DROPOUT,
                    nheads = NUM_HEADS
                   ).double()
    
    optimizer = torch.optim.Adam(model.parameters(), lr = LR)
    criterion = nn.MSELoss()
    for epoch in range(MAX_EPOCH):
        print("%d,%d,%d,%d,%.2f,%.4f,%d" % (epoch, 
                           training_length, 
                           forecast_window,
                           NUM_LAYERS,
                           DROPOUT,
                           LR,
                           NUM_HEADS
                           ), end = "")
        
        avg_miss = 0.0
        epoch_loss = 0.0
        avg_pred = 0.0
        avg_target = 0.0
        for batch, (_input, target) in enumerate(train_dataloader):
            optimizer.zero_grad()
            src = _input.double()
            target = _input.double()

            if target.shape[1] != training_length:
                continue
            
            prediction = model(src)
            avg_miss += np.mean(np.abs(prediction.detach().numpy() - target[:,:,-1].unsqueeze(-1).detach().numpy()))
            avg_pred += np.mean(prediction.detach().numpy())
            avg_target += np.mean(target[:,:,-1].unsqueeze(-1).detach().numpy())
            loss = criterion(prediction, target[:,:,-1].unsqueeze(-1))
            loss = torch.sqrt(loss)
            loss.backward()
            optimizer.step()
            epoch_loss += loss.detach().item()
        print("%4.4f,%4.4f,%4.4f,%4.4f" % (epoch_loss / (batch + 1),
                                              avg_miss / (batch + 1),
                                              avg_pred / (batch + 1),
                                              avg_target / (batch + 1)
                                              ))
