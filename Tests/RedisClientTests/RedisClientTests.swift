//
//  RedisClientTests.swift
//  RedisClient
//
//  Created by Valerio Mazzeo on 08/03/2017.
//  Copyright © 2017 VMLabs Limited. All rights reserved.
//
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU Lesser General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
//
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
//  See the GNU Lesser General Public License for more details.
//
//  You should have received a copy of the GNU Lesser General Public License
//  along with this program. If not, see <http://www.gnu.org/licenses/>.
//

import XCTest
@testable import RedisClient

class RedisClientTests: XCTestCase {

    static let allTests = [
        ("testExecute", testExecute),
        ("testGET", testGET),
        ("testGETNilValue", testGETNilValue),
        ("testGETError", testGETError),
        ("testINCR", testINCR),
        ("testINCRError", testINCRError),
        ("testDEL", testDEL),
        ("testDELError", testDELError),
        ("testLPUSH", testLPUSH),
        ("testLPUSHError", testLPUSHError),
        ("testRPUSH", testRPUSH),
        ("testRPUSHError", testRPUSHError),
        ("testRPOPLPUSH", testRPOPLPUSH),
        ("testRPOPLPUSHWithEmptyList", testRPOPLPUSHWithEmptyList),
        ("testRPOPLPUSHError", testRPOPLPUSHError),
        ("testBRPOPLPUSH", testBRPOPLPUSH),
        ("testBRPOPLPUSHError", testBRPOPLPUSHError),
        ("testSETEX", testSETEX),
        ("testSETEXError", testSETEXError),
        ("testLREM", testLREM),
        ("testLREMError", testLREMError),
        ("testLRANGE", testLRANGE),
        ("testLRANGEError", testLRANGEError),
        ("testZADD", testZADD),
        ("testZADDError", testZADDError),
        ("testZRANGE", testZRANGE),
        ("testZRANGEError", testZRANGEError),
        ("testZRANGEBYSCORE", testZRANGEBYSCORE),
        ("testZRANGEBYSCOREError", testZRANGEBYSCOREError),
        ("testZREM", testZREM),
        ("testZREMError", testZREMError),
        ("testEnqueueTransaction", testEnqueueTransaction),
        ("testMULTI", testMULTI),
        ("testMULTIError", testMULTIError),
        ("testEnqueueError", testEnqueueError),
        ("testEXECError", testEXECError)
    ]

    func testExecute() throws {

        let client = MockClient()

        let expectation = self.expectation(description: "execute")

        client.execute = { command, arguments in
            XCTAssertEqual(command, "TEST")
            XCTAssertEqual(arguments!, ["arg1", "arg2"])
            expectation.fulfill()

            return RedisClientResponse.null
        }

        _ = try client.execute("TEST", arguments: "arg1", "arg2")

        self.waitForExpectations(timeout: 4.0, handler: nil)
    }

    func testGET() throws {

        let client = MockClient()

        let expectation = self.expectation(description: "execute")

        client.execute = { command, arguments in
            XCTAssertEqual(command, "GET")
            XCTAssertEqual(arguments?[0], "test")
            expectation.fulfill()

            return RedisClientResponse.string("value")
        }

        let value = try client.get("test")

        self.waitForExpectations(timeout: 4.0, handler: nil)

        XCTAssertEqual(value, "value")
    }

    func testGETNilValue() throws {

        let client = MockClient()

        let expectation = self.expectation(description: "execute")

        client.execute = { command, arguments in
            XCTAssertEqual(command, "GET")
            XCTAssertEqual(arguments?[0], "test")
            expectation.fulfill()

            return RedisClientResponse.null
        }

        let value = try client.get("test")

        self.waitForExpectations(timeout: 4.0, handler: nil)

        XCTAssertNil(value)
    }

    func testGETError() throws {

        let client = MockClient()

        let expectation = self.expectation(description: "execute")

        client.execute = { command, arguments in
            expectation.fulfill()

            return RedisClientResponse.error("error")
        }

        XCTAssertThrowsError(try client.get("test"), "get") { error in
            XCTAssertTrue(error is RedisClientError)
        }

        self.waitForExpectations(timeout: 4.0, handler: nil)
    }

    func testINCR() throws {

        let client = MockClient()

        let expectation = self.expectation(description: "execute")

        client.execute = { command, arguments in
            XCTAssertEqual(command, "INCR")
            XCTAssertEqual(arguments?[0], "test")
            expectation.fulfill()

            return RedisClientResponse.integer(1)
        }

        let value = try client.incr("test")

        self.waitForExpectations(timeout: 4.0, handler: nil)

        XCTAssertEqual(value, 1)
    }

    func testINCRError() throws {

        let client = MockClient()

        let expectation = self.expectation(description: "execute")

        client.execute = { command, arguments in
            expectation.fulfill()

            return RedisClientResponse.error("error")
        }

        XCTAssertThrowsError(try client.incr("test"), "incr") { error in
            XCTAssertTrue(error is RedisClientError)
        }

        self.waitForExpectations(timeout: 4.0, handler: nil)
    }

    func testDEL() throws {

        let client = MockClient()

        let expectation = self.expectation(description: "execute")

        client.execute = { command, arguments in
            XCTAssertEqual(command, "DEL")
            XCTAssertEqual(arguments?[0], "key1")
            XCTAssertEqual(arguments?[1], "key2")
            XCTAssertEqual(arguments?[2], "key3")
            expectation.fulfill()

            return RedisClientResponse.integer(3)
        }

        let value = try client.del("key1", "key2", "key3")

        self.waitForExpectations(timeout: 4.0, handler: nil)

        XCTAssertEqual(value, 3)
    }

    func testDELError() throws {

        let client = MockClient()

        let expectation = self.expectation(description: "execute")

        client.execute = { command, arguments in
            expectation.fulfill()

            return RedisClientResponse.error("error")
        }

        XCTAssertThrowsError(try client.del("test"), "del") { error in
            XCTAssertTrue(error is RedisClientError)
        }

        self.waitForExpectations(timeout: 4.0, handler: nil)
    }

    func testLPUSH() throws {

        let client = MockClient()

        let expectation = self.expectation(description: "execute")

        client.execute = { command, arguments in
            XCTAssertEqual(command, "LPUSH")
            XCTAssertEqual(arguments?[0], "test")
            XCTAssertEqual(arguments?[1], "a")
            XCTAssertEqual(arguments?[2], "b")
            XCTAssertEqual(arguments?[3], "c")
            expectation.fulfill()

            return RedisClientResponse.integer(3)
        }

        let count = try client.lpush("test", values: "a", "b", "c")

        self.waitForExpectations(timeout: 4.0, handler: nil)

        XCTAssertEqual(count, 3)
    }

    func testLPUSHError() throws {

        let client = MockClient()

        let expectation = self.expectation(description: "execute")

        client.execute = { command, arguments in
            expectation.fulfill()

            return RedisClientResponse.null
        }

        XCTAssertThrowsError(try client.lpush("test", values: "a", "b", "c"), "lpush") { error in
            XCTAssertTrue(error is RedisClientError)
        }

        self.waitForExpectations(timeout: 4.0, handler: nil)
    }

    func testRPUSH() throws {

        let client = MockClient()

        let expectation = self.expectation(description: "execute")

        client.execute = { command, arguments in
            XCTAssertEqual(command, "RPUSH")
            XCTAssertEqual(arguments?[0], "test")
            XCTAssertEqual(arguments?[1], "a")
            XCTAssertEqual(arguments?[2], "b")
            XCTAssertEqual(arguments?[3], "c")
            expectation.fulfill()

            return RedisClientResponse.integer(3)
        }

        let count = try client.rpush("test", values: "a", "b", "c")

        self.waitForExpectations(timeout: 4.0, handler: nil)

        XCTAssertEqual(count, 3)
    }

    func testRPUSHError() throws {

        let client = MockClient()

        let expectation = self.expectation(description: "execute")

        client.execute = { command, arguments in
            expectation.fulfill()

            return RedisClientResponse.null
        }

        XCTAssertThrowsError(try client.rpush("test", values: "a", "b", "c"), "rpush") { error in
            XCTAssertTrue(error is RedisClientError)
        }
        
        self.waitForExpectations(timeout: 4.0, handler: nil)
    }

    func testRPOPLPUSH() throws {

        let client = MockClient()

        let expectation = self.expectation(description: "execute")

        client.execute = { command, arguments in
            XCTAssertEqual(command, "RPOPLPUSH")
            XCTAssertEqual(arguments?[0], "test")
            XCTAssertEqual(arguments?[1], "anotherTest")
            expectation.fulfill()

            return RedisClientResponse.string("a")
        }

        let item = try client.rpoplpush(source: "test", destination: "anotherTest")

        self.waitForExpectations(timeout: 4.0, handler: nil)

        XCTAssertEqual(item, "a")
    }

    func testRPOPLPUSHWithEmptyList() throws {

        let client = MockClient()

        let expectation = self.expectation(description: "execute")

        client.execute = { command, arguments in
            XCTAssertEqual(command, "RPOPLPUSH")
            XCTAssertEqual(arguments?[0], "test")
            XCTAssertEqual(arguments?[1], "anotherTest")
            expectation.fulfill()

            return RedisClientResponse.null
        }

        let item = try client.rpoplpush(source: "test", destination: "anotherTest")

        self.waitForExpectations(timeout: 4.0, handler: nil)

        XCTAssertNil(item)
    }

    func testRPOPLPUSHError() throws {

        let client = MockClient()

        let expectation = self.expectation(description: "execute")

        client.execute = { command, arguments in
            XCTAssertEqual(command, "RPOPLPUSH")
            XCTAssertEqual(arguments?[0], "test")
            XCTAssertEqual(arguments?[1], "anotherTest")
            expectation.fulfill()

            return RedisClientResponse.error("error")
        }

        XCTAssertThrowsError(try client.rpoplpush(source: "test", destination: "anotherTest"), "rpoplpush") { error in
            XCTAssertTrue(error is RedisClientError)
        }

        self.waitForExpectations(timeout: 4.0, handler: nil)
    }

    func testBRPOPLPUSH() throws {

        let client = MockClient()

        let expectation = self.expectation(description: "execute")

        client.execute = { command, arguments in
            XCTAssertEqual(command, "BRPOPLPUSH")
            XCTAssertEqual(arguments?[0], "test")
            XCTAssertEqual(arguments?[1], "anotherTest")
            expectation.fulfill()

            return RedisClientResponse.string("a")
        }

        let item = try client.brpoplpush(source: "test", destination: "anotherTest")

        self.waitForExpectations(timeout: 4.0, handler: nil)

        XCTAssertEqual(item, "a")
    }

    func testBRPOPLPUSHError() throws {

        let client = MockClient()

        let expectation = self.expectation(description: "execute")

        client.execute = { command, arguments in
            XCTAssertEqual(command, "BRPOPLPUSH")
            XCTAssertEqual(arguments?[0], "test")
            XCTAssertEqual(arguments?[1], "anotherTest")
            expectation.fulfill()

            return RedisClientResponse.null
        }

        XCTAssertThrowsError(try client.brpoplpush(source: "test", destination: "anotherTest"), "brpoplpush") { error in
            XCTAssertTrue(error is RedisClientError)
        }

        self.waitForExpectations(timeout: 4.0, handler: nil)
    }

    func testSETEX() throws {

        let client = MockClient()

        let expectation = self.expectation(description: "execute")

        client.execute = { command, arguments in
            XCTAssertEqual(command, "SETEX")
            XCTAssertEqual(arguments?[0], "test")
            XCTAssertEqual(arguments?[1], "60")
            XCTAssertEqual(arguments?[2], "a")
            expectation.fulfill()

            return RedisClientResponse.status(.ok)
        }

        try client.setex("test", timeout: 60.0, value: "a")

        self.waitForExpectations(timeout: 4.0, handler: nil)
    }

    func testSETEXError() throws {

        let client = MockClient()

        let expectation = self.expectation(description: "execute")

        client.execute = { command, arguments in
            XCTAssertEqual(command, "SETEX")
            XCTAssertEqual(arguments?[0], "test")
            XCTAssertEqual(arguments?[1], "60")
            XCTAssertEqual(arguments?[2], "a")
            expectation.fulfill()

            return RedisClientResponse.error("error")
        }

        XCTAssertThrowsError(try client.setex("test", timeout: 60.0, value: "a"), "setex") { error in
            XCTAssertTrue(error is RedisClientError)
        }

        self.waitForExpectations(timeout: 4.0, handler: nil)
    }

    func testLREM() throws {

        let client = MockClient()

        let expectation = self.expectation(description: "execute")

        client.execute = { command, arguments in
            XCTAssertEqual(command, "LREM")
            XCTAssertEqual(arguments?[0], "test")
            XCTAssertEqual(arguments?[1], "-5")
            XCTAssertEqual(arguments?[2], "a")
            expectation.fulfill()

            return RedisClientResponse.integer(1)
        }

        let value = try client.lrem("test", value: "a", count: -5)

        self.waitForExpectations(timeout: 4.0, handler: nil)

        XCTAssertEqual(value, 1)
    }

    func testLREMError() throws {

        let client = MockClient()

        let expectation = self.expectation(description: "execute")

        client.execute = { command, arguments in
            XCTAssertEqual(command, "LREM")
            XCTAssertEqual(arguments?[0], "test")
            XCTAssertEqual(arguments?[1], "-5")
            XCTAssertEqual(arguments?[2], "a")
            expectation.fulfill()

            return RedisClientResponse.error("error")
        }

        XCTAssertThrowsError(try client.lrem("test", value: "a", count: -5), "lrem") { error in
            XCTAssertTrue(error is RedisClientError)
        }

        self.waitForExpectations(timeout: 4.0, handler: nil)
    }

    func testLRANGE() throws {

        let client = MockClient()

        let expectation = self.expectation(description: "execute")

        client.execute = { command, arguments in
            XCTAssertEqual(command, "LRANGE")
            XCTAssertEqual(arguments?[0], "test")
            XCTAssertEqual(arguments?[1], "0")
            XCTAssertEqual(arguments?[2], "-1")
            expectation.fulfill()

            return RedisClientResponse.array([.string("one"), .string("two")])
        }

        let value = try client.lrange("test", start: 0, stop: -1)

        self.waitForExpectations(timeout: 4.0, handler: nil)

        XCTAssertEqual(value, ["one", "two"])
    }

    func testLRANGEError() throws {

        let client = MockClient()

        let expectation = self.expectation(description: "execute")

        client.execute = { command, arguments in
            XCTAssertEqual(command, "LRANGE")
            XCTAssertEqual(arguments?[0], "test")
            XCTAssertEqual(arguments?[1], "0")
            XCTAssertEqual(arguments?[2], "-1")
            expectation.fulfill()

            return RedisClientResponse.error("error")
        }

        XCTAssertThrowsError(try client.lrange("test", start: 0, stop: -1), "lrange") { error in
            XCTAssertTrue(error is RedisClientError)
        }

        self.waitForExpectations(timeout: 4.0, handler: nil)
    }

    func testZADD() throws {

        let client = MockClient()

        let expectation = self.expectation(description: "execute")

        client.execute = { command, arguments in
            XCTAssertEqual(command, "ZADD")
            XCTAssertEqual(arguments?[0], "test")
            XCTAssertEqual(arguments?[1], "1.0")
            XCTAssertEqual(arguments?[2], "a")
            XCTAssertEqual(arguments?[3], "2.0")
            XCTAssertEqual(arguments?[4], "b")
            expectation.fulfill()

            return RedisClientResponse.integer(2)
        }

        try client.zadd("test", values: (score: 1, member: "a"), (score: 2, member: "b"))

        self.waitForExpectations(timeout: 4.0, handler: nil)
    }

    func testZADDError() throws {

        let client = MockClient()

        let expectation = self.expectation(description: "execute")

        client.execute = { command, arguments in
            XCTAssertEqual(command, "ZADD")
            XCTAssertEqual(arguments?[0], "test")
            XCTAssertEqual(arguments?[1], "1.0")
            XCTAssertEqual(arguments?[2], "a")
            XCTAssertEqual(arguments?[3], "2.0")
            XCTAssertEqual(arguments?[4], "b")
            expectation.fulfill()

            return RedisClientResponse.error("error")
        }

        XCTAssertThrowsError(try client.zadd("test", values: [(score: 1, member: "a"), (score: 2, member: "b")]), "lrem") { error in
            XCTAssertTrue(error is RedisClientError)
        }

        self.waitForExpectations(timeout: 4.0, handler: nil)
    }

    func testZRANGE() throws {

        let client = MockClient()

        let expectation = self.expectation(description: "execute")

        client.execute = { command, arguments in
            XCTAssertEqual(command, "ZRANGE")
            XCTAssertEqual(arguments?[0], "test")
            XCTAssertEqual(arguments?[1], "0")
            XCTAssertEqual(arguments?[2], "-1")
            expectation.fulfill()

            return RedisClientResponse.array([.string("one"), .string("two")])
        }

        let value = try client.zrange("test", start: 0, stop: -1)

        self.waitForExpectations(timeout: 4.0, handler: nil)

        XCTAssertEqual(value, ["one", "two"])
    }

    func testZRANGEError() throws {

        let client = MockClient()

        let expectation = self.expectation(description: "execute")

        client.execute = { command, arguments in
            XCTAssertEqual(command, "ZRANGE")
            XCTAssertEqual(arguments?[0], "test")
            XCTAssertEqual(arguments?[1], "0")
            XCTAssertEqual(arguments?[2], "-1")
            expectation.fulfill()

            return RedisClientResponse.error("error")
        }

        XCTAssertThrowsError(try client.zrange("test", start: 0, stop: -1), "zrange") { error in
            XCTAssertTrue(error is RedisClientError)
        }
        
        self.waitForExpectations(timeout: 4.0, handler: nil)
    }

    func testZRANGEBYSCORE() throws {

        let client = MockClient()

        let expectation = self.expectation(description: "execute")

        client.execute = { command, arguments in
            XCTAssertEqual(command, "ZRANGEBYSCORE")
            XCTAssertEqual(arguments?[0], "test")
            XCTAssertEqual(arguments?[1], "0.0")
            XCTAssertEqual(arguments?[2], "10.0")
            expectation.fulfill()

            return RedisClientResponse.array([.string("one"), .string("two")])
        }

        let value = try client.zrangebyscore("test", min: 0.0, max: 10.0, includeMin: true, includeMax: true)

        self.waitForExpectations(timeout: 4.0, handler: nil)

        XCTAssertEqual(value, ["one", "two"])
    }

    func testZRANGEBYSCOREError() throws {

        let client = MockClient()

        let expectation = self.expectation(description: "execute")

        client.execute = { command, arguments in
            XCTAssertEqual(command, "ZRANGEBYSCORE")
            XCTAssertEqual(arguments?[0], "test")
            XCTAssertEqual(arguments?[1], "(0.0")
            XCTAssertEqual(arguments?[2], "(10.0")
            expectation.fulfill()

            return RedisClientResponse.error("error")
        }

        XCTAssertThrowsError(try client.zrangebyscore("test", min: 0.0, max: 10.0, includeMin: false, includeMax: false), "zrangebyscore") { error in
            XCTAssertTrue(error is RedisClientError)
        }
        
        self.waitForExpectations(timeout: 4.0, handler: nil)
    }

    func testZREM() throws {

        let client = MockClient()

        let expectation = self.expectation(description: "execute")

        client.execute = { command, arguments in
            XCTAssertEqual(command, "ZREM")
            XCTAssertEqual(arguments?[0], "test")
            XCTAssertEqual(arguments?[1], "a")
            expectation.fulfill()

            return RedisClientResponse.integer(1)
        }

        let value = try client.zrem("test", member: "a")

        self.waitForExpectations(timeout: 4.0, handler: nil)

        XCTAssertEqual(value, 1)
    }

    func testZREMError() throws {

        let client = MockClient()

        let expectation = self.expectation(description: "execute")

        client.execute = { command, arguments in
            XCTAssertEqual(command, "ZREM")
            XCTAssertEqual(arguments?[0], "test")
            XCTAssertEqual(arguments?[1], "a")
            expectation.fulfill()

            return RedisClientResponse.error("error")
        }

        XCTAssertThrowsError(try client.zrem("test", member: "a"), "zrem") { error in
            XCTAssertTrue(error is RedisClientError)
        }
        
        self.waitForExpectations(timeout: 4.0, handler: nil)
    }

    func testEnqueueTransaction() throws {

        let transaction = RedisClientTransaction()

        let expectation = self.expectation(description: "enqueue")

        try transaction.enqueue {
            defer { expectation.fulfill() }
            throw RedisClientError.invalidResponse(RedisClientResponse.status(.queued))
        }

        self.waitForExpectations(timeout: 4.0, handler: nil)
    }

    func testMULTI() throws {

        let client = MockClient()

        var commands = [String]()

        client.execute = { command, arguments in

            commands.append(command)

            switch command {
            case "MULTI":
                return RedisClientResponse.status(.ok)
            case "EXEC":
                return RedisClientResponse.array([.status(.ok)])
            default:
                throw RedisClientError.invalidResponse(RedisClientResponse.status(.queued))
            }
        }

        let responses = try client.multi { client, transaction in
            try transaction.enqueue { _ = try client.execute("COMMAND1", arguments: nil) }
            try transaction.enqueue { _ = try client.execute("COMMAND2", arguments: nil) }
        }

        XCTAssertEqual(responses.first?.status, .ok)
        XCTAssertEqual(commands[0], "MULTI")
        XCTAssertEqual(commands[1], "COMMAND1")
        XCTAssertEqual(commands[2], "COMMAND2")
        XCTAssertEqual(commands[3], "EXEC")
    }

    func testMULTIError() throws {

        let client = MockClient()

        var commands = [String]()

        client.execute = { command, arguments in

            commands.append(command)

            switch command {
            case "MULTI":
                return RedisClientResponse.error("error")
            case "EXEC":
                return RedisClientResponse.array([.status(.ok)])
            default:
                throw RedisClientError.invalidResponse(RedisClientResponse.status(.queued))
            }
        }

        XCTAssertThrowsError(try client.multi { client, transaction in
            try transaction.enqueue { _ = try client.execute("COMMAND1", arguments: nil) }
            try transaction.enqueue { _ = try client.execute("COMMAND2", arguments: nil) }
        }) { error in
            XCTAssertTrue(error is RedisClientError)
        }
    }

    func testEnqueueError() throws {

        let client = MockClient()

        var commands = [String]()

        client.execute = { command, arguments in

            commands.append(command)

            switch command {
            case "MULTI":
                return RedisClientResponse.status(.ok)
            case "DISCARD":
                return RedisClientResponse.status(.ok)
            case "EXEC":
                return RedisClientResponse.array([.status(.ok)])
            default:
                throw RedisClientError.invalidResponse(RedisClientResponse.error("error"))
            }
        }

        XCTAssertThrowsError(try client.multi { client, transaction in
            try transaction.enqueue { _ = try client.execute("COMMAND1", arguments: nil) }
            try transaction.enqueue { _ = try client.execute("COMMAND2", arguments: nil) }
        }) { error in
            switch error as? RedisClientError {
            case .some(.transactionAborted):
                break
            default:
                XCTFail()
            }
        }
    }

    func testEXECError() throws {

        let client = MockClient()

        var commands = [String]()

        client.execute = { command, arguments in

            commands.append(command)

            switch command {
            case "MULTI":
                return RedisClientResponse.status(.ok)
            case "EXEC":
                return RedisClientResponse.error("error")
            default:
                throw RedisClientError.invalidResponse(RedisClientResponse.status(.queued))
            }
        }

        XCTAssertThrowsError(try client.multi { client, transaction in
            try transaction.enqueue { _ = try client.execute("COMMAND1", arguments: nil) }
            try transaction.enqueue { _ = try client.execute("COMMAND2", arguments: nil) }
        }) { error in
            XCTAssertTrue(error is RedisClientError)
        }
    }
}

extension RedisClientTests {

    class MockClient: RedisClient {

        var execute: ((String, [String]?) throws -> RedisClientResponse)?

        func execute(_ command: String, arguments: [String]?) throws -> RedisClientResponse {

            return try self.execute?(command, arguments) ?? RedisClientResponse.null
        }
    }
}
