# -*- coding: utf-8 -*-
"""
Created on Fri Jan 18 22:12:49 2019

@author: conan
"""

import numpy as np
import operator
from os import listdir

def img2vector(filename):
    # 创建向量
    returnVect = np.zeros((1, 1024))
    # 打开数据文件，读取每行内容
    fr = open(filename)
    for i in range(32):
        # 读取每一行
        lineStr = fr.readline()
        # 将每行前 32 字符转成 int 存入向量
        for j in range(32):
            returnVect[0, 32*i+j] = int(lineStr[j])
            
    return returnVect

def classifier(inX,dataSet,labels,k):
    #compute diatances
    dataSetSize=dataSet.shape[0]
    diffMat=np.tile(inX,(dataSetSize,1))-dataSet
    sqDiffMat=diffMat**2
    sqDistances=sqDiffMat.sum(axis=1)
    #distances is a vector
    distances=sqDistances**0.5
    
    sortedDistIndicies=distances.argsort()
    classCount={}
    
    for i in range(k):
        voteIlabel=labels[sortedDistIndicies[i]]
        classCount[voteIlabel]=classCount.get(voteIlabel,0)+1
        
    sortedClassCount=sorted(classCount.items(),key=operator.itemgetter(1), reverse=True)
    
    return sortedClassCount[0][0]

    
class handwritingClassTest():
    #sample labels
    hwLabels=[]
    trainingFileList=listdir('digits/trainingDigits')
    m=len(trainingFileList)
    
    trainingMat=np.zeros((m,1024))
    
    for i in range(m):
        fileNameStr=trainingFileList[i]
        fileStr=fileNameStr.spilt('.')[0]
        classNumStr=int(fileStr.spilt('_')[0])
        hwLabels.append(classNumStr)
        trainingMat[i, :] = img2vector(
            'digits/trainingDigits/%s' % fileNameStr)
       # traingingMat[i,:]=ima2vector('digits/trainingDigits/%s' % fileNameStr))
        
    testFileList=listdir('digits/testDigits')
    
    errorCount=0.0
    mTest=len(testFileList)
    
    for i in range(mTest):
        fileNameStr=testFileList[i]
        fileStr=fileNameStr.spilt('.')[0]
        classNumStr=int(fileStr.spilt('_')[0])
        
        vectorUnderTest=img2vector('digits/testDigits/%s' % fileNameStr)
        
        classifierResult=classifier(vectorUnderTest)
        
        print("测试样本 %d, 分类器预测: %d, 真实类别: %d" %
              (i+1, classifierResult, classNumStr))
              
        if(classifierResult!=classNumStr):
            errorCount+=1.0
    
    print("\n错误分类计数: %d" % errorCount)
    print("\n错误分类比例: %f" % (errorCount/float(mTest)))


handwritingClassTest()          
    