import pandas as pd
import numpy as np

def fit(x, combinatorial):
    min_chi2=999999.
    for y in combinatorial:
        segment, residuals, _, _, _ = np.polyfit(x, y, 1, full=True)
        # only segments with a reasonable slope are considered 
        #if abs(np.poly1d(segment).c[0])>0.2: continue
        chi2 = residuals/(len(x)-2)
        if  chi2 < min_chi2:
            min_chi2 = chi2
            best_segment = np.poly1d(segment)
            best_combination = y
    return best_segment, {"x": x, "y":best_combination}

def segment_reconstructor(chamberhits):
    
    # get the hits in the 4 layers. Empty array in case there are none in a given layer
    layerhits = np.array([
        chamberhits[chamberhits.LAYER==1].X_POS_LEFT.tolist() + chamberhits[chamberhits.LAYER==1].X_POS_RIGHT.tolist(),
        chamberhits[chamberhits.LAYER==2].X_POS_LEFT.tolist() + chamberhits[chamberhits.LAYER==2].X_POS_RIGHT.tolist(),
        chamberhits[chamberhits.LAYER==3].X_POS_LEFT.tolist() + chamberhits[chamberhits.LAYER==3].X_POS_RIGHT.tolist(),
        chamberhits[chamberhits.LAYER==4].X_POS_LEFT.tolist() + chamberhits[chamberhits.LAYER==4].X_POS_RIGHT.tolist()
        ])

    # the case with hits in 4 layers
    if len(chamberhits.Z_POS.unique())==4:
            x = np.array([chamberhits[chamberhits.LAYER==1].Z_POS.unique()[0],
                          chamberhits[chamberhits.LAYER==2].Z_POS.unique()[0],
                          chamberhits[chamberhits.LAYER==3].Z_POS.unique()[0],
                          chamberhits[chamberhits.LAYER==4].Z_POS.unique()[0]])
            combinatorial = np.array(np.meshgrid(layerhits[0], layerhits[1], layerhits[2], layerhits[3])).T.reshape(-1,4)
    # alternatively get the hits in at least3 layers.
    elif len(chamberhits.Z_POS.unique())==3:
            layers_with_hits = [i for i,j in enumerate(layerhits) if len(j)!=0]
            x = np.array([chamberhits[chamberhits.LAYER==layers_with_hits[0]+1].Z_POS.unique()[0],
                          chamberhits[chamberhits.LAYER==layers_with_hits[1]+1].Z_POS.unique()[0],
                          chamberhits[chamberhits.LAYER==layers_with_hits[2]+1].Z_POS.unique()[0]])
            combinatorial = np.array(np.meshgrid(layerhits[layers_with_hits[0]],
                                                 layerhits[layers_with_hits[1]],
                                                 layerhits[layers_with_hits[2]])).T.reshape(-1,3)
    else:
        return None, None

    # fit (the best combination is ignored for the moment)
    segment, _ = fit(x, combinatorial)
    return segment, _

def segments_reconstructor(hits_list, run_id):
    hits = pd.DataFrame(hits_list)
    # input is a list of hits
    segments = {}
    for chamber in hits.SL.unique():
        chamberhits = hits[(hits.SL==chamber)]
        segments[chamber], _ = segment_reconstructor(chamberhits)

    return {'segments': segments, 'run_id': run_id}