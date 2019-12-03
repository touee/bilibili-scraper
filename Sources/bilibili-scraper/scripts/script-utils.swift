
import Foundation
import BilibiliAPI

class CSVReader<T: StringProtocol>: IteratorProtocol {
    typealias Element = [String]
    
    let string: T
    var cursor: T.Index
    
    public init(forString string: T) {
        self.string = string
        self.cursor = string.startIndex
    }
    
    deinit {
        print("i dead")
    }
    
    public func next() -> [String]? {
        if let result = nextRow() {
            self.cursor = self.string.index(after: self.cursor)
            return result
        }
        return nil
    }
    
    private func nextRow() -> [String]? {
        if self.string.endIndex == self.cursor {
            return nil
        } else if self.string[self.cursor] == "\n" {
            return [""]
        }
        
        var result = [String]()
        while true {
            let columnResult = nextFieldContent()
            result.append(columnResult)
            if self.string.endIndex == self.cursor
                || self.string[self.cursor] == "\n" {
                return result
            }
            self.cursor = self.string.index(after: self.cursor)
        }
    }
    
    func nextFieldContent() -> String {
        if self.string.endIndex == self.cursor
            || ["\n",  ","].contains(self.string[self.cursor]) {
            return ""
        }
        
        let isEscaped = self.string[self.cursor] == "\""
        if !isEscaped {
            let beginIndex = self.cursor
            while (self.cursor != self.string.endIndex
                &&  ![",", "\n"].contains(self.string[self.cursor])) {
                    self.cursor = self.string.index(after: self.cursor)
            }
            return String(self.string[beginIndex..<self.cursor])
        }
        
        var remained = self.string[self.string.index(after: self.cursor)..<self.string.endIndex]
        while true {
            guard var quoteIndex = remained.firstIndex(of: "\"") else {
                fatalError("Double quotation mark mismatched.")
            }
            var continuousQuotations = 0
            while quoteIndex != remained.endIndex
                && String(remained[quoteIndex]).unicodeScalars.first! == "\"" { // U+200D
                    continuousQuotations += 1
                    quoteIndex = remained.index(after: quoteIndex)
            }
            if continuousQuotations % 2 == 0 {
                remained = remained[quoteIndex..<remained.endIndex]
                continue
            }
            if quoteIndex == remained.endIndex
                || [",", "\n"].contains(remained[quoteIndex]) {
                let result = self.string[self.string.index(after: self.cursor)..<remained.index(before: quoteIndex)]
                self.cursor = quoteIndex
                return result.replacingOccurrences(of: "\"\"", with: "\"")
            }
            fatalError("Double quotation mark mismatched.")
        }
    }
}
