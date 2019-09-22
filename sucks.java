package com.pg.timer;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.PersistJobDataAfterExecution;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.quartz.QuartzJobBean;

import com.parablu.pcbd.domain.Cloud;
import com.pg.domain.FileInfo;
import com.pg.element.DeletedUser;
import com.pg.element.DeletedUsersElement;
import com.pg.helper.constant.GeneralHelperConstant;
import com.pg.helper.constant.PCHelperConstant;
import com.pg.helper.utils.MemoryStore;
import com.pg.service.UploadService;
import com.pg.service.UtilService;

@PersistJobDataAfterExecution
@DisallowConcurrentExecution
public class BackupUploadLatestJob extends QuartzJobBean implements Job {

	private static Logger logger = LoggerFactory.getLogger(BackupUploadLatestJob.class);

	private static final String UPLOAD = "/upload/";
	public static final String ENCRYPTED = "/encrypted/";
	public static final String CHUNK = "/chunk/";
	private static final String BLACK_LIST_USERS = "BlackListUsers";
	private static List<FileInfo> fileInfoList = null;
	private TimerTask backupTimerTask = null;

	private Timer backupTimer = null;

	private UploadService uploadService;

	private UtilService utilService;
	
	private Set<String> filesUnderProcess = new HashSet<>();

	public void setUploadService(UploadService uploadService) {
		this.uploadService = uploadService;
	}

	public void setUtilService(UtilService utilService) {
		this.utilService = utilService;
	}

	@Override
	protected void executeInternal(JobExecutionContext arg0) throws JobExecutionException {
		logger.debug("@@@@BackupUploadJob started ..... "+PCHelperConstant.getODBCallFrequency());
		ExecutorService executor = null;
		try {
			boolean isJobsShouldStop = PCHelperConstant.isJobsStopEnabled();
 			if(isJobsShouldStop){
 				logger.debug("stopJobsEnabled in privacygateway.properties so return");
 				return;
 			}
			Cloud cloud = utilService.getCloud(1);
			List<DeletedUsersElement> deletedUsersElements = utilService.getAllDeletedUsers(1);
			List<String> deletedUserList = getDeletedUsersFromElements(deletedUsersElements);
			long threadSize = utilService.getThreadSize(cloud.getCloudId(), cloud.getCloudName());
			if (threadSize == 0) {
				threadSize = PCHelperConstant.getThreadLimit();
			}
			logger.debug(" threads val........" + threadSize);
			final int threadSizeVal = (int) threadSize;
			executor = Executors.newFixedThreadPool(threadSizeVal);
		
			fileInfoList = uploadService.getFilesForUpload(cloud.getCloudName());
			CompletionService<String> pool = new ExecutorCompletionService<>(executor);
			checkThreadStatusAndStartUpload(cloud, executor, pool, deletedUserList);
			for (int i = 0; i < threadSizeVal; i++) {
				logger.debug("Creating thread for first time>>>>>>>>> i value::" + i);
				callUploadFiles(cloud, executor, pool, deletedUserList);
			}

			logger.debug(" exit upload part..........");
		} catch (Exception e) {
			logger.error(" exception in backup upload job......."+e.getMessage());
			logger.trace("exception in backup upload job ......." + e);
		}
	}
	
	private void callUploadFiles(Cloud cloud, ExecutorService executor, CompletionService<String> pool, List<String> deletedUserList) {
		logger.debug("Files to backup ............... :");
		Runnable uploadJob = () -> {
			uploadFiles(cloud, executor, pool,deletedUserList);
		};
		pool.submit(uploadJob, "");
	}

	
	private void uploadFiles(
			Cloud cloud, ExecutorService executor,
			CompletionService<String> pool, List<String> deletedUserList) {
		do {
			logger.debug("....inside while loop..........");
			FileInfo fileInfo = null;
			try {
				logger.debug("message list......................................." + fileInfoList.size());
				fileInfo = getFileForProcessing(cloud, deletedUserList);
				if (fileInfo != null) {
					processMessage(fileInfo, cloud);
					logger.error(fileInfo.getId() + " %%%%%%% upload completed for file ...." + fileInfo.getFileName());
				} 
				logger.debug("Thread ready for next File .... " + fileInfoList.size());
			} catch (Exception e) {
				logger.error("exception inside BackupUploadJob .... " + e.getMessage());
				logger.trace("exception inside BackupUploadJob ...." + e);
			}
			if (fileInfo != null && StringUtils.isNotEmpty(fileInfo.getId())) {
				filesUnderProcess.remove(fileInfo.getId());
			}
		}while(!CollectionUtils.isEmpty(fileInfoList));
		logger.error("no files to upload so wait and then retry>>>>");
		try {
			Thread.sleep(30000);
		} catch (InterruptedException e) {
			//
		}
		callUploadFiles(cloud, executor, pool, deletedUserList);
	}
	
	
	private synchronized FileInfo getFileForProcessing(Cloud cloud, List<String> deletedUserList) {
	
		if (CollectionUtils.isEmpty(fileInfoList)) {
			fileInfoList = getMessagesToProcess(cloud, deletedUserList);
			if(CollectionUtils.isEmpty(fileInfoList)){
				return null;
			}
		}
		FileInfo fileInfo  = fileInfoList.get(0);
		if (fileInfo != null) {
			if (filesUnderProcess.contains(fileInfo.getId())) {
				fileInfoList.remove(fileInfo);
				return getFileForProcessing(cloud, deletedUserList);
			} else {
				fileInfoList.remove(fileInfo);
				filesUnderProcess.add(fileInfo.getId());
			}
		}

		logger.debug("...after list size....." + fileInfoList.size());
		return fileInfo;
	}

	private List<FileInfo> getMessagesToProcess(Cloud cloud, List<String> deletedUserList) {

		List<FileInfo> latestList = uploadService.getFilesForUpload(cloud.getCloudName());
		int i = 0;
		for (FileInfo fileInfo : latestList) {

			String userName = fileInfo.getUserName().toLowerCase();
			if (deletedUserList.contains(userName)) {
				logger.debug("it contains deleted user so remove from message queue.....");
				acknowlwdgeAndCloseSession(cloud, fileInfo);
				continue;
			}
			Object obj = MemoryStore.get(BLACK_LIST_USERS);
			if (obj != null) {
				Set<String> blackUsersList = (Set<String>) obj;
				if (blackUsersList.contains(userName)) {
					logger.debug("it contains #blackList# user so remove from message queue.....userName:"
							+ fileInfo.getUserName());
					uploadService.moveFailedFilesToBkpQueue(cloud.getCloudName(), fileInfo);
					uploadService.deleteUploadedFiles(fileInfo, cloud.getCloudName());
					continue;
				}
			}
			String userBlockedForWrite = "429_" + fileInfo.getUserName();
			Object userBlockedFor429 = MemoryStore.get(userBlockedForWrite);
			if (userBlockedFor429 != null) {
				uploadService.moveFailedFilesToBkpQueue(cloud.getCloudName(), fileInfo);
				uploadService.deleteUploadedFiles(fileInfo, cloud.getCloudName());
				continue;
			}
			String unMappedUser = "404_" + fileInfo.getUserName();
			Object unMappedUser404 = MemoryStore.get(unMappedUser);
			if (unMappedUser404 != null) {
				utilService.saveFailedFile(cloud.getCloudId(), fileInfo);
				uploadService.deleteUploadedFiles(fileInfo, cloud.getCloudName());
				continue;
			}

			fileInfoList.add(fileInfo);
			i = i + 1;
		}

		return fileInfoList;
	}

	public void checkThreadStatusAndStartUpload( Cloud cloud,
			ExecutorService executor, CompletionService<String> pool, List<String> deletedUserList) {
		if (backupTimer == null) {
			backupTimer = new Timer();
		}
		if (backupTimerTask == null) {
			backupTimerTask = new TimerTask() {
				@Override
				public void run() {
					try {
						logger.error("Check the task is completed>>>>>>>>");
						Future<String> future = pool.take();
						if (future.isDone()) {
							logger.debug("Thread is completed so assign new task>>>>>>>>>>>");
							callUploadFiles( cloud, executor, pool, deletedUserList);
						}
					} catch (Exception e) {
						logger.error("Error in checkThreadStatusAndStartUpload", e);
						logger.trace("" + e);
					}
				}
			};
			backupTimer.schedule(backupTimerTask, 1000, 1000);
		}
	}

	private boolean uploadFile(FileInfo fileInfo, Cloud cloud) {
		boolean isFileUploaded = false;
		try {
			isFileUploaded = uploadService.uploadAllFilesToCloud(cloud.getCloudName(), fileInfo, cloud, null);
		} catch (Exception e) {
			logger.debug("" + e);
			logger.error(fileInfo.getFileName() + "#####FILE NOT UPLOADED SUCCESSFULLY .. " + e.getMessage());
		}
		return isFileUploaded;
	}

	private void deleteUnReferencedChunk(String deviceUUID, String cloudName, String name, String batchId,
			String fileName) {
		if (!StringUtils.isEmpty(deviceUUID)) {
            String parabluBaseMountFolder = PCHelperConstant.getParabluFolderBasePath();
			String chunkPath = parabluBaseMountFolder + cloudName + UPLOAD + deviceUUID + CHUNK;
			String encryptedPath = parabluBaseMountFolder + cloudName + UPLOAD + deviceUUID + ENCRYPTED;
			String filePath = parabluBaseMountFolder + cloudName + UPLOAD + deviceUUID + GeneralHelperConstant.CLOUD_PATH_SEPARATOR;
			if (!StringUtils.isEmpty(batchId)) {
				chunkPath = parabluBaseMountFolder + cloudName + UPLOAD + deviceUUID + GeneralHelperConstant.CLOUD_PATH_SEPARATOR + batchId
						+ CHUNK;
				encryptedPath = parabluBaseMountFolder + cloudName + UPLOAD + deviceUUID + GeneralHelperConstant.CLOUD_PATH_SEPARATOR
						+ batchId + ENCRYPTED;
				filePath = parabluBaseMountFolder + cloudName + UPLOAD + deviceUUID + GeneralHelperConstant.CLOUD_PATH_SEPARATOR + batchId
						+ GeneralHelperConstant.CLOUD_PATH_SEPARATOR;
			}
			try {
				File deleteChunkFile = new File(chunkPath + name);
				File deleteEncryptedFile = new File(encryptedPath + name);
				if (deleteChunkFile.exists()) {
					deleteChunkFile.delete();
				}
				if (deleteEncryptedFile.exists()) {
					deleteEncryptedFile.delete();
				}

				File deleteOrgFile = new File(filePath + fileName);
				if (deleteOrgFile.exists()) {
					deleteOrgFile.delete();
				}
			} catch (Exception e) {
				logger.error("Error trying to clean files ..... " + e.getMessage());
				logger.trace("" + e);
			}
		}
	}
	private void deleteEncryptedChunks(String deviceUUID, String cloudName, String name, String batchId,
			String fileName) {
		if (!StringUtils.isEmpty(deviceUUID)) {
			String parabluBaseMountFolder = PCHelperConstant.getParabluFolderBasePath();

			String encryptedPath = parabluBaseMountFolder + cloudName + UPLOAD + deviceUUID + ENCRYPTED;

			if (!StringUtils.isEmpty(batchId)) {
				encryptedPath = parabluBaseMountFolder + cloudName + UPLOAD + deviceUUID
						+ GeneralHelperConstant.CLOUD_PATH_SEPARATOR + batchId + ENCRYPTED;
			}
			try {

				File deleteEncryptedFile = new File(encryptedPath + name);

				if (deleteEncryptedFile.exists()) {
					deleteEncryptedFile.delete();
				}

			} catch (Exception e) {
				logger.error("Error trying to clean files ..... " + e.getMessage());
				logger.trace("" + e);
			}
		}
	}
	public void processMessage(FileInfo fileInfo,  Cloud cloud) {
	
		try {
			logger.debug("Processing " + fileInfo.getFileName());
			String userBlockedForWrite = "429_" + fileInfo.getUserName();
			Object userBlockedFor429 = MemoryStore.get(userBlockedForWrite);		
			String userUnMapped = "404_" + fileInfo.getUserName();
			Object userUnMapped404 = MemoryStore.get(userUnMapped);	
			
			if(userBlockedFor429!=null){
				logger.error(fileInfo.getUserName() + "........user acct has too many requests ........move file to bkp queue .............");
 				uploadService.moveFailedFilesToBkpQueue(cloud.getCloudName(), fileInfo);
 				uploadService.deleteUploadedFiles(fileInfo, cloud.getCloudName());
			}else if(userUnMapped404 != null){
				logger.error(fileInfo.getUserName() + "........unmapped user.............");
				utilService.saveFailedFile(cloud.getCloudId(),fileInfo );
				uploadService.deleteUploadedFiles(fileInfo, cloud.getCloudName());
			}else{
 				boolean isFileUploaded  = uploadFile(fileInfo, cloud);
				if (isFileUploaded) {
					acknowlwdgeAndCloseSession(cloud, fileInfo);
				} else {
 					logger.error(fileInfo.getFileName() + "................move file to bkp queue .............");
 					uploadService.moveFailedFilesToBkpQueue(cloud.getCloudName(), fileInfo);
 					uploadService.deleteUploadedFiles(fileInfo, cloud.getCloudName());
 				}
 				logger.debug(" message status.............. " + isFileUploaded);
 			}
		} catch (Exception e) {
			logger.trace("" + e);
			logger.error("Exception inside BackupUploadJob Processor PooledConnectionFactory !" + e.getMessage());
		}
		deleteFileEncryptedChunks(cloud, fileInfo);
	}

	private void deleteFileEncryptedChunks(Cloud cloud, FileInfo fileInfo) {
		
		if(fileInfo==null)
			return;
		for (String name : fileInfo.getChunkFiles()) {
			int occurance = StringUtils.countMatches(name, ".");
			if (occurance > 1) {
				name = name.substring(0, name.lastIndexOf('.'));
			}
			deleteEncryptedChunks(fileInfo.getDeviceUUID(), cloud.getCloudName(), name, fileInfo.getBatchId(),
					fileInfo.getFileName());

		}
	}

	private void acknowlwdgeAndCloseSession(Cloud cloud, FileInfo fileInfo) {
		
		for (String name : fileInfo.getChunkFiles()) {
			int occurance = StringUtils.countMatches(name, ".");
			if (occurance > 1) {
				name = name.substring(0, name.lastIndexOf('.'));
			}
			deleteUnReferencedChunk(fileInfo.getDeviceUUID(), cloud.getCloudName(), name,
					fileInfo.getBatchId(), fileInfo.getFileName());
		}
		uploadService.deleteUploadedFiles(fileInfo, cloud.getCloudName());
		logger.debug(fileInfo.getFileName()+".....file deleted ..... "+fileInfo.getId());
	}
	private List<String> getDeletedUsersFromElements(List<DeletedUsersElement> deletedUsersElements) {
		List<String> delUsers = new ArrayList<>();
		for(DeletedUsersElement deletedUsersElement :deletedUsersElements) {
			for(DeletedUser deletedUser : deletedUsersElement.getDeletedUsers()) {
				
				delUsers.add(deletedUser.getUserName().toLowerCase());
			}
			
		}
		return delUsers;
	}
}
