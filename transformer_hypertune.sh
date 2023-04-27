#!/bin/sh
for e in 10 20
do
    for lr in 1e-3 1e-4
    do
        for nl in 1 4
        do
            for tl in 24 48 
            do
                for fw in 24 48
                do
                    for bs in 1 128
                    do
                        python south_centra_wz_transformer_tacc.py --max_epochs ${e} --lr ${lr} --num_layers ${nl}  --training_length ${tl} --forecast_window ${fw} --batch_size ${bs} >> transformer_hypertuning.csv
                    done
                done
            done
        done
    done
done