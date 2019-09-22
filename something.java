
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
