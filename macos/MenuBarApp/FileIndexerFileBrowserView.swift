import SwiftUI
import AppKit

extension FileIndexerView {
    func toggleFolderFiles(_ folder: IndexedFolder) {
        withAnimation {
            if expandedFolders.contains(folder.path) {
                expandedFolders.remove(folder.path)
            } else {
                expandedFolders.insert(folder.path)
            }
        }

        guard expandedFolders.contains(folder.path) else { return }
        Task {
            await watchManager.loadIndexedFiles(for: folder.path)
        }
    }

    @ViewBuilder
    func folderFilesSection(_ folder: IndexedFolder) -> some View {
        if watchManager.loadingFilesForFolders.contains(folder.path) {
            HStack(spacing: 8) {
                ProgressView()
                    .scaleEffect(0.8)
                Text("Loading indexed files...")
                    .font(.caption)
                    .foregroundColor(.secondary)
            }
            .padding(.top, 6)
        } else {
            let files = watchManager.folderFiles[folder.path] ?? []
            VStack(alignment: .leading, spacing: 8) {
                Text("Files (\(files.count))")
                    .font(.caption)
                    .foregroundColor(.secondary)

                if files.isEmpty {
                    Text("No indexed files found yet.")
                        .font(.caption2)
                        .foregroundColor(.secondary)
                        .padding(.vertical, 4)
                } else {
                    ForEach(files) { file in
                        folderFileRow(folder: folder, file: file)
                    }
                }
            }
            .padding(8)
            .background(Color(NSColor.windowBackgroundColor).opacity(0.95))
            .overlay(
                RoundedRectangle(cornerRadius: 6)
                    .stroke(Color.secondary.opacity(0.25), lineWidth: 1)
            )
            .cornerRadius(6)
        }
    }

    func folderFileRow(folder: IndexedFolder, file: IndexedFolderFile) -> some View {
        VStack(alignment: .leading, spacing: 6) {
            HStack(spacing: 8) {
                Image(systemName: "doc.text")
                    .foregroundColor(.secondary)
                VStack(alignment: .leading, spacing: 2) {
                    Text(file.name)
                        .font(.caption)
                        .fontWeight(.medium)
                        .foregroundColor(.primary)
                    Text(file.relativePath)
                        .font(.caption2)
                        .foregroundColor(.secondary)
                        .lineLimit(1)
                        .truncationMode(.middle)
                }
                Spacer()
                Button(action: {
                    NSWorkspace.shared.open(URL(fileURLWithPath: file.path))
                }) {
                    Image(systemName: "arrow.up.forward.square")
                        .font(.caption)
                }
                .buttonStyle(.plain)
                .help("Open file")
            }

            HStack(spacing: 4) {
                ForEach(file.inheritedTags, id: \.self) { tag in
                    HStack(spacing: 2) {
                        Image(systemName: "lock.fill")
                            .font(.caption2)
                        Text(tag)
                            .font(.caption2)
                    }
                    .foregroundColor(.primary)
                    .padding(.horizontal, 6)
                    .padding(.vertical, 2)
                    .background(Color.accentColor.opacity(0.32))
                    .cornerRadius(4)
                    .help("Inherited from folder. Remove it on the folder, not the file.")
                }

                ForEach(file.fileTags, id: \.self) { tag in
                    HStack(spacing: 2) {
                        Text(tag)
                            .font(.caption2)
                        if !folder.isIndexing {
                            Button(action: {
                                removeFileTag(tag, from: file, in: folder)
                            }) {
                                Image(systemName: "xmark.circle.fill")
                                    .font(.caption2)
                            }
                            .buttonStyle(.plain)
                        }
                    }
                    .foregroundColor(.primary)
                    .padding(.horizontal, 6)
                    .padding(.vertical, 2)
                    .background(Color.green.opacity(0.32))
                    .cornerRadius(4)
                }
            }

            HStack(spacing: 8) {
                TextField("Add file tag...", text: Binding(
                    get: { newFileTagTextByPath[file.path, default: ""] },
                    set: { newFileTagTextByPath[file.path] = $0 }
                ))
                .textFieldStyle(.roundedBorder)
                .font(.caption)
                .disabled(folder.isIndexing || isUpdatingTags)
                .onSubmit {
                    addFileTag(to: file, in: folder)
                }

                Button(action: {
                    addFileTag(to: file, in: folder)
                }) {
                    Image(systemName: "plus.circle.fill")
                }
                .buttonStyle(.plain)
                .disabled(folder.isIndexing || isUpdatingTags || (newFileTagTextByPath[file.path, default: ""].trimmingCharacters(in: .whitespacesAndNewlines).isEmpty))
            }
        }
        .padding(8)
        .background(Color(NSColor.controlBackgroundColor))
        .overlay(
            RoundedRectangle(cornerRadius: 6)
                .stroke(Color.secondary.opacity(0.2), lineWidth: 1)
        )
        .cornerRadius(6)
    }

    func addFileTag(to file: IndexedFolderFile, in folder: IndexedFolder) {
        guard !folder.isIndexing else { return }
        let tag = newFileTagTextByPath[file.path, default: ""].trimmingCharacters(in: .whitespacesAndNewlines)
        guard !tag.isEmpty else { return }

        isUpdatingTags = true
        Task {
            do {
                try await watchManager.addTagToFile(folderPath: folder.path, filePath: file.path, tag: tag)
                await MainActor.run {
                    newFileTagTextByPath[file.path] = ""
                    isUpdatingTags = false
                }
            } catch {
                await MainActor.run {
                    watchManager.error = "Failed to add file tag: \(error.localizedDescription)"
                    isUpdatingTags = false
                }
            }
        }
    }

    func removeFileTag(_ tag: String, from file: IndexedFolderFile, in folder: IndexedFolder) {
        guard !folder.isIndexing else { return }
        guard !file.inheritedTags.contains(tag) else { return }

        isUpdatingTags = true
        Task {
            do {
                try await watchManager.removeTagFromFile(folderPath: folder.path, filePath: file.path, tag: tag)
                await MainActor.run {
                    isUpdatingTags = false
                }
            } catch {
                await MainActor.run {
                    watchManager.error = "Failed to remove file tag: \(error.localizedDescription)"
                    isUpdatingTags = false
                }
            }
        }
    }
}
